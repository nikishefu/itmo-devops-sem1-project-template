package main

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Price struct {
	ID         int
	Name       string
	Category   string
	Price      float64
	CreateDate string
}

type PriceRepository struct {
	db *sql.DB
}

func NewPriceRepository(db *sql.DB) *PriceRepository {
	return &PriceRepository{db: db}
}

func (r *PriceRepository) BatchInsert(ctx context.Context, prices []Price) (int, int, float64, error) {
	var total_items, cats int
	var total_price float64

	if len(prices) == 0 {
		err := r.db.QueryRowContext(ctx,
			`SELECT COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices`,
		).Scan(&cats, &total_price)
		return 0, cats, total_price, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return total_items, cats, total_price, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO prices(name,category,price,create_date)
		 VALUES ($1,$2,$3,$4)`)
	if err != nil {
		return total_items, cats, total_price, err
	}
	defer stmt.Close()

	for _, p := range prices {
		if _, err := stmt.ExecContext(ctx,
			p.Name, p.Category, p.Price, p.CreateDate,
		); err != nil {
			return total_items, cats, total_price, err
		}
		total_items++
	}

	err = tx.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices`,
	).Scan(&cats, &total_price)
	if err != nil {
		return total_items, cats, total_price, err
	}

	return total_items, cats, total_price, tx.Commit()
}

func (r *PriceRepository) ReadAll(ctx context.Context) ([]Price, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id,name,category,price,create_date FROM prices ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Price
	for rows.Next() {
		var p Price
		if err := rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.CreateDate); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, rows.Err()
}

type PriceService struct{ repo *PriceRepository }

func NewPriceService(r *PriceRepository) *PriceService { return &PriceService{repo: r} }

func (s *PriceService) ImportZip(ctx context.Context, r io.ReaderAt, size int64) (map[string]any, error) {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}

	var batch []Price

	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}

		cr := csv.NewReader(rc)

		if _, err := cr.Read(); err != nil {
			rc.Close()
			return nil, err
		}

		for {
			rec, err := cr.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				rc.Close()
				return nil, err
			}

			id, _ := strconv.Atoi(rec[0])
			price, _ := strconv.ParseFloat(rec[3], 64)

			batch = append(batch, Price{
				ID:         id,
				Name:       rec[1],
				Category:   rec[2],
				Price:      price,
				CreateDate: rec[4],
			})
		}
		rc.Close()
	}

	total_items, cats, total_price, err := s.repo.BatchInsert(ctx, batch)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"total_items":      total_items,
		"total_categories": cats,
		"total_price":      total_price,
	}, nil
}

func (s *PriceService) ExportZip(ctx context.Context, w io.Writer) error {
	data, err := s.repo.ReadAll(ctx)
	if err != nil {
		return err
	}

	zw := zip.NewWriter(w)
	f, err := zw.Create("data.csv")
	if err != nil {
		return err
	}

	cw := csv.NewWriter(f)
	if err := cw.Write([]string{"id", "name", "category", "price", "create_date"}); err != nil {
		return err
	}

	for _, p := range data {
		if err := cw.Write([]string{
			strconv.Itoa(p.ID),
			p.Name,
			p.Category,
			strconv.FormatFloat(p.Price, 'f', -1, 64),
			p.CreateDate,
		}); err != nil {
			return err
		}
	}

	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}

	return zw.Close()
}

type Handler struct{ svc *PriceService }

func NewHandler(s *PriceService) *Handler { return &Handler{svc: s} }

func (h *Handler) PostPrices(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read upload", http.StatusInternalServerError)
		return
	}

	reader := bytes.NewReader(data)

	res, err := h.svc.ImportZip(r.Context(), reader, int64(len(data)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("Error: failed to write response: %v", err)
	}
}

func (h *Handler) GetPrices(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=prices.zip")

	if err := h.svc.ExportZip(r.Context(), w); err != nil {
		log.Printf("Export error: %v", err)
	}
}

func openDB() (*sql.DB, error) {
	dsn := "postgres://" + os.Getenv("POSTGRES_USER") + ":" + os.Getenv("POSTGRES_PASSWORD") +
		"@" + os.Getenv("POSTGRES_HOST") + ":" + os.Getenv("POSTGRES_PORT") +
		"/" + os.Getenv("POSTGRES_DB")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	return db, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS prices (
		id SERIAL PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		category VARCHAR(255) NOT NULL,
		price DECIMAL(10,2) NOT NULL,
		create_date TIMESTAMP NOT NULL
	);`)
	return err
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := ensureSchema(ctx, db); err != nil {
		cancel()
		return err
	}
	cancel()

	repo := NewPriceRepository(db)
	svc := NewPriceService(repo)
	h := NewHandler(svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			h.PostPrices(w, r)
		case http.MethodGet:
			h.GetPrices(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)

	go func() {
		log.Println("Server listening on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-quit:
		log.Println("Shutting down server...")
	case err := <-errCh:
		return err
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}

	log.Println("Server exiting")
	return nil
}
