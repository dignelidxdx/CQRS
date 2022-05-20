package database

import (
	"context"
	"database/sql"

	models "github.com/dignelidxdx/models"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLRepository struct {
	db *sql.DB
}

func NewMySQLRepository(url string) (*MySQLRepository, error) {
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return &MySQLRepository{db}, nil
}

func (repo *MySQLRepository) Close() {
	repo.db.Close()
}

func (repo *MySQLRepository) InsertFeed(ctx context.Context, feed *models.Feed) error {
	_, err := repo.db.ExecContext(ctx, "INSERT INTO feeds (id, title, description) VALUES (?, ?, ?)", feed.ID, feed.Title, feed.Description)
	return err
}

func (repo *MySQLRepository) ListFeeds(ctx context.Context) ([]*models.Feed, error) {
	rows, err := repo.db.QueryContext(ctx, "SELECT id, title, description, created_at FROM feeds")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	feeds := []*models.Feed{}
	for rows.Next() {
		feed := &models.Feed{}
		if err := rows.Scan(&feed.ID, &feed.Title, &feed.Description, &feed.CreatedAt); err != nil {
			return nil, err
		}
		feeds = append(feeds, feed)
	}
	return feeds, nil
}
