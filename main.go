package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type User struct {
	ID                     int        `db:"id"`
	Name                   string     `db:"name"`
	LatestCheckinCreatedAt *time.Time `db:"latest_checkin_created_at"`
}

type TagScore struct {
	TagID          int     `db:"tag_id"`
	TagName        string  `db:"tag_name"`
	NormalizedName string  `db:"normalized_name"`
	Score          float64 `db:"score"`
}

type Result struct {
	User       User
	Similarity float64
}

func main() {
	var (
		target   = flag.String("target", "", "target user")
		sample   = flag.Int("sample", 30, "checkin samples")
		activeIn = flag.Int("active-in", 12, "active user in n months")
	)
	flag.Parse()
	if *target == "" {
		os.Exit(1)
	}

	if err := godotenv.Load(); err != nil {
		panic(err)
	}

	db, err := sqlx.Connect("postgres", os.Getenv("DB_DSN"))
	if err != nil {
		panic(err)
	}

	users, err := queryUsers(db, *activeIn)
	if err != nil {
		panic(err)
	}

	scores := make(map[int][]TagScore)
	for _, u := range users {
		ts, err := calcScore(db, &u, *sample)
		if err != nil {
			panic(err)
		}
		scores[u.ID] = ts
	}

	for _, u := range users {
		if u.Name == *target {
			ranking(u, users, scores)
			break
		}
	}
}

func queryUsers(db *sqlx.DB, activeInMonths int) ([]User, error) {
	var users []User
	err := db.Select(&users, `
SELECT u.id, u.name, max(e.created_at) latest_checkin_created_at
FROM users u
         LEFT JOIN ejaculations e ON u.id = e.user_id
WHERE is_protected = false
  AND accept_analytics = true
GROUP BY u.id, u.name
ORDER BY id
`)
	if err != nil {
		return nil, err
	}

	aYearAgo := time.Now().AddDate(0, -activeInMonths, 0)

	var filtered []User
	for _, u := range users {
		if u.LatestCheckinCreatedAt != nil && u.LatestCheckinCreatedAt.After(aYearAgo) {
			filtered = append(filtered, u)
		}
	}

	return filtered, nil
}

func calcScore(db *sqlx.DB, u *User, samples int) ([]TagScore, error) {
	var ts []TagScore
	err := db.Select(&ts, `
WITH recent_used_tags AS (SELECT used_tags_with_score.tag_id, 
                                 max(score) AS max_score, 
                                 count(*) AS count
                          FROM ejaculations e
                                   INNER JOIN
                               (SELECT e.id AS ejaculation_id,
                                       et.tag_id,
                                       1.0  AS score
                                FROM ejaculations e
                                         JOIN ejaculation_tag et ON e.id = et.ejaculation_id
                                WHERE user_id = $1
                                UNION
                                SELECT e.id AS ejaculation_id,
                                       mt.tag_id,
                                       0.5 AS score
                                FROM ejaculations e
                                         JOIN metadata m ON e.normalized_link = m.url
                                         JOIN metadata_tag mt ON m.url = mt.metadata_url
                                WHERE user_id = $1) AS used_tags_with_score
                               ON e.id = used_tags_with_score.ejaculation_id
                                   INNER JOIN (SELECT e.id AS ejaculation_id
                                               FROM ejaculations e
                                               WHERE e.user_id = $1
                                               ORDER BY e.ejaculated_date DESC
                                               LIMIT $2) AS recent_ejaculation_ids
                                              ON e.id = recent_ejaculation_ids.ejaculation_id
                          GROUP BY used_tags_with_score.tag_id)
SELECT t.id AS tag_id, t.name AS tag_name, t.normalized_name AS normalized_name, max_score AS score
FROM ((SELECT * FROM recent_used_tags WHERE max_score = 1.0 ORDER BY count DESC LIMIT 10)
      UNION
      (SELECT * FROM recent_used_tags WHERE max_score = 0.5 ORDER BY count DESC LIMIT 5)) AS tag_scores
         INNER JOIN tags t ON tag_scores.tag_id = t.id
ORDER BY score DESC, count DESC, t.id
`, u.ID, samples)
	return ts, err
}

func ranking(u User, users []User, scores map[int][]TagScore) {
	var results []Result

	ts1 := scores[u.ID]

	for _, u2 := range users {
		if u.ID == u2.ID {
			continue
		}

		ts2 := scores[u2.ID]
		s := similarity(ts1, ts2)
		results = append(results, Result{
			User:       u2,
			Similarity: s,
		})
	}

	sort.Slice(results, func(i, j int) bool { return results[j].Similarity < results[i].Similarity })

	fmt.Printf("==> %s\n", u.Name)
	for i := 0; i < len(results) && i < 10; i++ {
		r := results[i]
		fmt.Printf("%s: %f\n", r.User.Name, r.Similarity)
	}
}

func similarity(ts1, ts2 []TagScore) float64 {
	var v1, v2 []float64

outer:
	for _, s1 := range ts1 {
		v1 = append(v1, s1.Score/float64(len(ts1)))

		for _, s2 := range ts2 {
			if s1.TagID == s2.TagID {
				v2 = append(v2, s2.Score/float64(len(ts2)))
				continue outer
			}
		}
		v2 = append(v2, 0.0)
	}

	l := len(v1)
	s := 0.0
	for i := 0; i < l; i++ {
		s += v1[i] * v2[i]
	}

	return s
}
