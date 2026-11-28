package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

// --- Global World Travel Persistence ---

func (s *Store) GetTravelState(ctx context.Context) (*model.TravelState, error) {
	row := s.readDB.QueryRowContext(ctx, `
		SELECT status, current_city_id, next_city_id, transport_mode,
		       arrived_at, depart_at, transit_eta,
		       weather_summary, weather_code, weather_temp_c, weather_updated_at,
		       last_journal_at, last_proactive_at,
		       leg_count, total_distance_km, started_at, updated_at
		FROM travel_state WHERE id = 1
	`)
	var st model.TravelState
	var arrivedMs, departMs, etaMs, weatherMs, journalMs, proMs, startedMs, updatedMs int64
	err := row.Scan(
		&st.Status, &st.CurrentCityID, &st.NextCityID, &st.TransportMode,
		&arrivedMs, &departMs, &etaMs,
		&st.WeatherSummary, &st.WeatherCode, &st.WeatherTempC, &weatherMs,
		&journalMs, &proMs,
		&st.LegCount, &st.TotalDistanceKm, &startedMs, &updatedMs,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	st.ArrivedAt = fromUnixMs(arrivedMs)
	st.DepartAt = fromUnixMs(departMs)
	st.TransitETA = fromUnixMs(etaMs)
	st.WeatherUpdatedAt = fromUnixMs(weatherMs)
	st.LastJournalAt = fromUnixMs(journalMs)
	st.LastProactiveAt = fromUnixMs(proMs)
	st.StartedAt = fromUnixMs(startedMs)
	st.UpdatedAt = fromUnixMs(updatedMs)
	return &st, nil
}

func (s *Store) SaveTravelState(ctx context.Context, st *model.TravelState) error {
	if st == nil {
		return nil
	}
	now := time.Now()
	if st.StartedAt.IsZero() {
		st.StartedAt = now
	}
	st.UpdatedAt = now
	_, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO travel_state(
			id, status, current_city_id, next_city_id, transport_mode,
			arrived_at, depart_at, transit_eta,
			weather_summary, weather_code, weather_temp_c, weather_updated_at,
			last_journal_at, last_proactive_at,
			leg_count, total_distance_km, started_at, updated_at
		) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = excluded.status,
			current_city_id = excluded.current_city_id,
			next_city_id = excluded.next_city_id,
			transport_mode = excluded.transport_mode,
			arrived_at = excluded.arrived_at,
			depart_at = excluded.depart_at,
			transit_eta = excluded.transit_eta,
			weather_summary = excluded.weather_summary,
			weather_code = excluded.weather_code,
			weather_temp_c = excluded.weather_temp_c,
			weather_updated_at = excluded.weather_updated_at,
			last_journal_at = excluded.last_journal_at,
			last_proactive_at = excluded.last_proactive_at,
			leg_count = excluded.leg_count,
			total_distance_km = excluded.total_distance_km,
			started_at = excluded.started_at,
			updated_at = excluded.updated_at
	`,
		st.Status, st.CurrentCityID, st.NextCityID, st.TransportMode,
		toUnixMs(st.ArrivedAt), toUnixMs(st.DepartAt), toUnixMs(st.TransitETA),
		st.WeatherSummary, st.WeatherCode, st.WeatherTempC, toUnixMs(st.WeatherUpdatedAt),
		toUnixMs(st.LastJournalAt), toUnixMs(st.LastProactiveAt),
		st.LegCount, st.TotalDistanceKm, toUnixMs(st.StartedAt), toUnixMs(st.UpdatedAt),
	)
	return err
}

func (s *Store) AddTravelJournal(ctx context.Context, e *model.TravelJournalEntry) error {
	if e == nil {
		return nil
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	if e.Kind == "" {
		e.Kind = "diary"
	}
	res, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO travel_journal(city_id, city_name, country, status, weather, local_time, kind, content, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.CityID, e.CityName, e.Country, e.Status, e.Weather, e.LocalTime, e.Kind, e.Content, e.CreatedAt.UnixMilli())
	if err != nil {
		return err
	}
	e.ID, _ = res.LastInsertId()
	return nil
}

func (s *Store) ListTravelJournal(ctx context.Context, limit int) ([]model.TravelJournalEntry, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT id, city_id, city_name, country, status, weather, local_time, kind, content, created_at
		FROM travel_journal
		ORDER BY created_at DESC, id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTravelJournalRows(rows)
}

func (s *Store) ListTravelJournalByCity(ctx context.Context, cityID string, limit int) ([]model.TravelJournalEntry, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT id, city_id, city_name, country, status, weather, local_time, kind, content, created_at
		FROM travel_journal
		WHERE city_id = ?
		ORDER BY created_at DESC, id DESC
		LIMIT ?
	`, cityID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTravelJournalRows(rows)
}

func scanTravelJournalRows(rows *sql.Rows) ([]model.TravelJournalEntry, error) {
	var out []model.TravelJournalEntry
	for rows.Next() {
		var e model.TravelJournalEntry
		var ts int64
		if err := rows.Scan(&e.ID, &e.CityID, &e.CityName, &e.Country, &e.Status, &e.Weather, &e.LocalTime, &e.Kind, &e.Content, &ts); err != nil {
			return nil, err
		}
		e.CreatedAt = time.UnixMilli(ts)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) AddTravelVisit(ctx context.Context, v *model.TravelVisit) error {
	if v == nil {
		return nil
	}
	if v.ArrivedAt.IsZero() {
		v.ArrivedAt = time.Now()
	}
	res, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO travel_visits(city_id, city_name, country, lat, lon, transport_mode, distance_km, arrived_at, departed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, v.CityID, v.CityName, v.Country, v.Lat, v.Lon, v.TransportMode, v.DistanceKm, v.ArrivedAt.UnixMilli(), toUnixMs(v.DepartedAt))
	if err != nil {
		return err
	}
	v.ID, _ = res.LastInsertId()
	return nil
}

func (s *Store) UpdateLatestTravelVisitDeparture(ctx context.Context, cityID string, departedAt time.Time) error {
	_, err := s.writeDB.ExecContext(ctx, `
		UPDATE travel_visits
		SET departed_at = ?
		WHERE id = (
			SELECT id FROM travel_visits
			WHERE city_id = ? AND departed_at = 0
			ORDER BY arrived_at DESC, id DESC
			LIMIT 1
		)
	`, departedAt.UnixMilli(), cityID)
	return err
}

// ListTravelVisits returns visited cities in chronological order (oldest -> newest) when ascending=true,
// or reverse chronological order (newest -> oldest) when ascending=false.
func (s *Store) ListTravelVisits(ctx context.Context, limit int, ascending bool) ([]model.TravelVisit, error) {
	if limit <= 0 {
		limit = 200
	}
	order := "DESC"
	if ascending {
		order = "ASC"
	}
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT id, city_id, city_name, country, lat, lon, transport_mode, distance_km, arrived_at, departed_at
		FROM travel_visits
		ORDER BY arrived_at `+order+`, id `+order+`
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.TravelVisit
	for rows.Next() {
		var v model.TravelVisit
		var arrMs, depMs int64
		if err := rows.Scan(&v.ID, &v.CityID, &v.CityName, &v.Country, &v.Lat, &v.Lon, &v.TransportMode, &v.DistanceKm, &arrMs, &depMs); err != nil {
			return nil, err
		}
		v.ArrivedAt = fromUnixMs(arrMs)
		v.DepartedAt = fromUnixMs(depMs)
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) CountTravelStats(ctx context.Context) (cities int, countries int, err error) {
	err = s.readDB.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT city_id), COUNT(DISTINCT country) FROM travel_visits
	`).Scan(&cities, &countries)
	return cities, countries, err
}

func (s *Store) AddTravelSuggestion(ctx context.Context, sg *model.TravelSuggestion) error {
	if sg == nil {
		return nil
	}
	if sg.CreatedAt.IsZero() {
		sg.CreatedAt = time.Now()
	}
	if sg.Status == "" {
		sg.Status = "pending"
	}
	res, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO travel_suggestions(channel_id, member_id, member_name, city_id, city_name, note, status, created_at, visited_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, string(sg.ChannelID), sg.MemberID, sg.MemberName, sg.CityID, sg.CityName, sg.Note, sg.Status, sg.CreatedAt.UnixMilli(), toUnixMs(sg.VisitedAt))
	if err != nil {
		return err
	}
	sg.ID, _ = res.LastInsertId()
	return nil
}

func (s *Store) ListTravelSuggestions(ctx context.Context, status string, limit int) ([]model.TravelSuggestion, error) {
	if limit <= 0 {
		limit = 30
	}
	var rows *sql.Rows
	var err error
	if status != "" {
		rows, err = s.readDB.QueryContext(ctx, `
			SELECT id, channel_id, member_id, member_name, city_id, city_name, note, status, created_at, visited_at
			FROM travel_suggestions
			WHERE status = ?
			ORDER BY created_at ASC, id ASC
			LIMIT ?
		`, status, limit)
	} else {
		rows, err = s.readDB.QueryContext(ctx, `
			SELECT id, channel_id, member_id, member_name, city_id, city_name, note, status, created_at, visited_at
			FROM travel_suggestions
			ORDER BY created_at DESC, id DESC
			LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTravelSuggestionRows(rows)
}

// ListChannelSuggestionsForCity returns channel-scoped member suggestions for cityID so
// attribution never crosses channel boundaries.
func (s *Store) ListChannelSuggestionsForCity(ctx context.Context, ch model.ChannelID, cityID string) ([]model.TravelSuggestion, error) {
	rows, err := s.readDB.QueryContext(ctx, `
		SELECT id, channel_id, member_id, member_name, city_id, city_name, note, status, created_at, visited_at
		FROM travel_suggestions
		WHERE channel_id = ? AND city_id = ? AND status IN ('pending', 'selected', 'visited')
		ORDER BY created_at DESC
		LIMIT 5
	`, string(ch), cityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTravelSuggestionRows(rows)
}

func (s *Store) MarkTravelSuggestionsByCity(ctx context.Context, cityID, fromStatus, toStatus string, visitedAt time.Time) error {
	_, err := s.writeDB.ExecContext(ctx, `
		UPDATE travel_suggestions
		SET status = ?, visited_at = CASE WHEN ? > 0 THEN ? ELSE visited_at END
		WHERE city_id = ? AND status = ?
	`, toStatus, toUnixMs(visitedAt), toUnixMs(visitedAt), cityID, fromStatus)
	return err
}

func (s *Store) MarkTravelSuggestionStatus(ctx context.Context, id int64, status string) error {
	_, err := s.writeDB.ExecContext(ctx, `UPDATE travel_suggestions SET status = ? WHERE id = ?`, status, id)
	return err
}

func scanTravelSuggestionRows(rows *sql.Rows) ([]model.TravelSuggestion, error) {
	var out []model.TravelSuggestion
	for rows.Next() {
		var sg model.TravelSuggestion
		var chStr string
		var cMs, vMs int64
		if err := rows.Scan(&sg.ID, &chStr, &sg.MemberID, &sg.MemberName, &sg.CityID, &sg.CityName, &sg.Note, &sg.Status, &cMs, &vMs); err != nil {
			return nil, err
		}
		sg.ChannelID = model.ChannelID(chStr)
		sg.CreatedAt = fromUnixMs(cMs)
		sg.VisitedAt = fromUnixMs(vMs)
		out = append(out, sg)
	}
	return out, rows.Err()
}

func toUnixMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func fromUnixMs(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
