package main

import (
	"archive/zip"
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

func (r *PriceRepository) BatchInsert(ctx context.Context, prices []Price) error {
	if len(prices) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO prices(id,name,category,price,create_date)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			category = EXCLUDED.category,
			price = EXCLUDED.price,
			create_date = EXCLUDED.create_date`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range prices {
		if _, err := stmt.ExecContext(ctx,
			p.ID, p.Name, p.Category, p.Price, p.CreateDate,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *PriceRepository) StreamAll(ctx context.Context) (*sql.Rows, error) {
	return r.db.QueryContext(ctx,
		`SELECT id,name,category,price,create_date FROM prices ORDER BY id`)
}

func (r *PriceRepository) Stats(ctx context.Context) (int, int, float64, error) {
	var items, categories int
	var total float64

	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices`,
	).Scan(&items, &categories, &total)

	return items, categories, total, err
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

	if err := s.repo.BatchInsert(ctx, batch); err != nil {
		return nil, err
	}

	items, cats, total, err := s.repo.Stats(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"total_items":      items,
		"total_categories": cats,
		"total_price":      total,
	}, nil
}

func (s *PriceService) ExportZip(ctx context.Context, w io.Writer) error {
	rows, err := s.repo.StreamAll(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()

	zw := zip.NewWriter(w)
	f, _ := zw.Create("data.csv")
	cw := csv.NewWriter(f)

	cw.Write([]string{"id", "name", "category", "price", "create_date"})

	for rows.Next() {
		var p Price
		if err := rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.CreateDate); err != nil {
			return err
		}
		cw.Write([]string{
			strconv.Itoa(p.ID),
			p.Name,
			p.Category,
			strconv.FormatFloat(p.Price, 'f', -1, 64),
			p.CreateDate,
		})
	}

	cw.Flush()
	if err := rows.Err(); err != nil {
		return err
	}

	return zw.Close()
}

type Handler struct{ svc *PriceService }

func NewHandler(s *PriceService) *Handler { return &Handler{svc: s} }

func (h *Handler) PostPrices(w http.ResponseWriter, r *http.Request) {
	tmpFile, err := os.CreateTemp("", "upload-*.zip")
	if err != nil {
		http.Error(w, "Failed to create temp file", http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	size, err := io.Copy(tmpFile, r.Body)
	if err != nil {
		http.Error(w, "Failed to save upload", http.StatusInternalServerError)
		return
	}

	res, err := h.svc.ImportZip(r.Context(), tmpFile, size)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
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
		id INT PRIMARY KEY,
		name TEXT,
		category TEXT,
		price NUMERIC,
		create_date DATE
	);`)
	return err
}

func main() {
	db, err := openDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := ensureSchema(ctx, db); err != nil {
		cancel()
		log.Fatal(err)
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

	go func() {
		log.Println("Server listening on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}
