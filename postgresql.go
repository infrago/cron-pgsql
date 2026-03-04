package cron_postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/infrago/cron"
	"github.com/infrago/infra"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	infra.Register("postgres", &postgresDriver{})
	infra.Register("postgresql", &postgresDriver{})
	infra.Register("pgsql", &postgresDriver{})
}

type (
	postgresDriver struct{}

	postgresConnection struct {
		dsn        string
		schema     string
		jobsTable  string
		logsTable  string
		locksTable string
		pool       *pgxpool.Pool
	}
)

func (d *postgresDriver) Connection(inst *cron.Instance) (cron.Connection, error) {
	setting := inst.Config.Setting
	dsn := parseDSN(setting)

	schema := "public"
	if v, ok := setting["schema"].(string); ok && v != "" {
		schema = v
	}

	jobsTable := "cron_jobs"
	if v, ok := setting["jobs_table"].(string); ok && v != "" {
		jobsTable = v
	}

	logsTable := "cron_logs"
	if v, ok := setting["logs_table"].(string); ok && v != "" {
		logsTable = v
	}

	locksTable := "cron_locks"
	if v, ok := setting["locks_table"].(string); ok && v != "" {
		locksTable = v
	}

	return &postgresConnection{
		dsn:        dsn,
		schema:     schema,
		jobsTable:  jobsTable,
		logsTable:  logsTable,
		locksTable: locksTable,
	}, nil
}

func (c *postgresConnection) Open() error {
	pool, err := pgxpool.New(context.Background(), c.dsn)
	if err != nil {
		return err
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return err
	}
	c.pool = pool
	return c.ensureSchema()
}

func (c *postgresConnection) Close() error {
	if c.pool != nil {
		c.pool.Close()
	}
	return nil
}

func (c *postgresConnection) Add(name string, job cron.Job) error {
	job.Name = name
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	_, err = c.pool.Exec(context.Background(),
		fmt.Sprintf(
			`INSERT INTO %s (name, data, updated_at) VALUES ($1, $2::jsonb, now())
			 ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
			c.jobsTableSQL(),
		),
		name, data,
	)
	return err
}

func (c *postgresConnection) Enable(name string) error {
	_, err := c.pool.Exec(context.Background(),
		fmt.Sprintf(
			`UPDATE %s SET data = jsonb_set(data, '{disabled}', 'false'::jsonb, true), updated_at = now() WHERE name = $1`,
			c.jobsTableSQL(),
		),
		name,
	)
	return err
}

func (c *postgresConnection) Disable(name string) error {
	_, err := c.pool.Exec(context.Background(),
		fmt.Sprintf(
			`UPDATE %s SET data = jsonb_set(data, '{disabled}', 'true'::jsonb, true), updated_at = now() WHERE name = $1`,
			c.jobsTableSQL(),
		),
		name,
	)
	return err
}

func (c *postgresConnection) Remove(name string) error {
	tx, err := c.pool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	if _, err = tx.Exec(context.Background(),
		fmt.Sprintf(`DELETE FROM %s WHERE name = $1`, c.jobsTableSQL()),
		name,
	); err != nil {
		return err
	}

	if _, err = tx.Exec(context.Background(),
		fmt.Sprintf(`DELETE FROM %s WHERE job = $1`, c.logsTableSQL()),
		name,
	); err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func (c *postgresConnection) List() (map[string]cron.Job, error) {
	rows, err := c.pool.Query(context.Background(),
		fmt.Sprintf(`SELECT name, data FROM %s`, c.jobsTableSQL()),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]cron.Job{}
	for rows.Next() {
		var name string
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			return nil, err
		}
		var job cron.Job
		if err := json.Unmarshal(raw, &job); err != nil {
			continue
		}
		job.Name = name
		out[name] = job
	}
	return out, rows.Err()
}

func (c *postgresConnection) AppendLog(log cron.Log) error {
	data, err := json.Marshal(log)
	if err != nil {
		return err
	}
	_, err = c.pool.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO %s (job, data, created_at) VALUES ($1, $2::jsonb, now())`, c.logsTableSQL()),
		log.Job, data,
	)
	return err
}

func (c *postgresConnection) History(jobName string, offset, limit int) (int64, []cron.Log, error) {
	var total int64
	if err := c.pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT count(1) FROM %s WHERE job = $1`, c.logsTableSQL()),
		jobName,
	).Scan(&total); err != nil {
		return 0, nil, err
	}
	if total == 0 {
		return 0, []cron.Log{}, nil
	}

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = int(total)
	}

	rows, err := c.pool.Query(context.Background(),
		fmt.Sprintf(`SELECT data FROM %s WHERE job = $1 ORDER BY id DESC OFFSET $2 LIMIT $3`, c.logsTableSQL()),
		jobName, offset, limit,
	)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	logs := make([]cron.Log, 0, limit)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return 0, nil, err
		}
		var item cron.Log
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		logs = append(logs, item)
	}

	return total, logs, rows.Err()
}

func (c *postgresConnection) Lock(key string, ttl time.Duration) (bool, error) {
	_ = ttl

	var one int
	err := c.pool.QueryRow(context.Background(),
		fmt.Sprintf(
			`INSERT INTO %s (name, expired_at, updated_at)
			 VALUES ($1, now(), now())
			 ON CONFLICT (name) DO NOTHING
			 RETURNING 1`,
			c.locksTableSQL(),
		),
		key,
	).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func (c *postgresConnection) ensureSchema() error {
	ctx := context.Background()

	if _, err := c.pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(c.schema))); err != nil {
		return err
	}

	if _, err := c.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name TEXT PRIMARY KEY,
			data JSONB NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`, c.jobsTableSQL())); err != nil {
		return err
	}

	if _, err := c.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			job TEXT NOT NULL,
			data JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`, c.logsTableSQL())); err != nil {
		return err
	}

	if _, err := c.pool.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s ON %s (job, id DESC)
	`, quoteIdent(c.logsTable+"_job_id_idx"), c.logsTableSQL())); err != nil {
		return err
	}

	if _, err := c.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name TEXT PRIMARY KEY,
			expired_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`, c.locksTableSQL())); err != nil {
		return err
	}

	return nil
}

func (c *postgresConnection) jobsTableSQL() string {
	return quoteIdent(c.schema) + "." + quoteIdent(c.jobsTable)
}

func (c *postgresConnection) logsTableSQL() string {
	return quoteIdent(c.schema) + "." + quoteIdent(c.logsTable)
}

func (c *postgresConnection) locksTableSQL() string {
	return quoteIdent(c.schema) + "." + quoteIdent(c.locksTable)
}

func parseDSN(setting map[string]any) string {
	if v, ok := setting["dsn"].(string); ok && v != "" {
		return v
	}
	if v, ok := setting["url"].(string); ok && v != "" {
		return normalizePGURL(v)
	}

	host := "127.0.0.1"
	if v, ok := setting["host"].(string); ok && v != "" {
		host = v
	}
	port := "5432"
	if v, ok := setting["port"].(string); ok && v != "" {
		port = v
	}
	if v, ok := setting["port"].(int); ok && v > 0 {
		port = strconv.Itoa(v)
	}
	if v, ok := setting["port"].(int64); ok && v > 0 {
		port = strconv.FormatInt(v, 10)
	}

	user := "postgres"
	if v, ok := setting["username"].(string); ok && v != "" {
		user = v
	}
	if v, ok := setting["user"].(string); ok && v != "" {
		user = v
	}

	password := ""
	if v, ok := setting["password"].(string); ok {
		password = v
	}

	database := "postgres"
	if v, ok := setting["database"].(string); ok && v != "" {
		database = v
	}
	if v, ok := setting["dbname"].(string); ok && v != "" {
		database = v
	}

	sslmode := "disable"
	if v, ok := setting["sslmode"].(string); ok && v != "" {
		sslmode = v
	}

	u := &url.URL{
		Scheme: "postgres",
		Host:   host + ":" + port,
		Path:   "/" + database,
		User:   url.UserPassword(user, password),
	}
	query := u.Query()
	query.Set("sslmode", sslmode)
	u.RawQuery = query.Encode()
	return u.String()
}

func normalizePGURL(v string) string {
	if strings.HasPrefix(v, "pgsql://") {
		return "postgres://" + strings.TrimPrefix(v, "pgsql://")
	}
	if strings.HasPrefix(v, "postgresql://") {
		return "postgres://" + strings.TrimPrefix(v, "postgresql://")
	}
	return v
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
