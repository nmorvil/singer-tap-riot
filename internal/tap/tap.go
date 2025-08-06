package tap

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/KnutZuidema/golio"
	"github.com/KnutZuidema/golio/api"
	"github.com/nmorvil/singer-tap-riot/pkg/singer"
	"strconv"
	"sync"
	"time"
)

const (
	Matches        string = "matches"
	MatchTimelines string = "match_timelines"
	Elos           string = "elos"
	Accounts       string = "accounts"
)

// RiotServicePool manages multiple RiotService instances with different API keys
type RiotServicePool struct {
	services []*RiotService
	config   *Config
}

// PlayerGroup represents a group of players assigned to a specific API key
type PlayerGroup struct {
	Players []string
	Service *RiotService
}

func RunDiscovery(t *singer.Tap) error {
	catalog := CreateCatalog()
	return t.WriteCatalog(catalog)
}

func RunSync(t *singer.Tap, c *Config, cat *singer.Catalog, s *singer.State) error {
	pool := createRiotServicePool(c)
	playerGroups := pool.distributePlayersToServices(c.Players)

	var selectedStreams []string
	if cat == nil {
		selectedStreams = []string{Matches, MatchTimelines, Elos, Accounts}
	} else {
		selectedStreams = singer.GetSelectedStreams(cat)
	}

	for _, stream := range selectedStreams {
		switch stream {
		case Matches:
			t.Log("Starting sync of matches")
			syncMatchesConcurrent(t, playerGroups, s, c)
			break
		case MatchTimelines:
			t.Log("Starting sync of match timelines")
			syncMatchTimelinesConcurrent(t, playerGroups, s, c)
			break
		case Elos:
			t.Log("Starting sync of elos")
			syncElosConcurrent(t, playerGroups, s, c)
			break
		case Accounts:
			t.Log("Starting sync of accounts")
			syncAccountsConcurrent(t, playerGroups, s, c)
			break
		default:
			return errors.New("Unknown stream: " + stream)
		}
	}
	return nil
}

func CreateCatalog() *singer.Catalog {
	return &singer.Catalog{
		Streams: []singer.Stream{createMatchesStream(), createMatchTimelineStream(), createEloStream(), createAccountsStream()},
	}
}

func createRiotServicePool(c *Config) *RiotServicePool {
	services := make([]*RiotService, len(c.APIKeys))
	for i, apiKey := range c.APIKeys {
		services[i] = &RiotService{
			golio.NewClient(
				apiKey,
				golio.WithRegion(api.Region(c.Server)),
			),
			apiKey,
		}
	}
	return &RiotServicePool{
		services: services,
		config:   c,
	}
}

func (pool *RiotServicePool) distributePlayersToServices(players []string) []PlayerGroup {
	groups := make([]PlayerGroup, len(pool.services))

	for i := range groups {
		groups[i] = PlayerGroup{
			Players: make([]string, 0),
			Service: pool.services[i],
		}
	}

	for _, player := range players {
		serviceIndex := hashPlayerToServiceIndex(player, len(pool.services))
		groups[serviceIndex].Players = append(groups[serviceIndex].Players, player)
	}

	return groups
}

func hashPlayerToServiceIndex(player string, numServices int) int {
	hasher := md5.New()
	hasher.Write([]byte(player))
	hash := hex.EncodeToString(hasher.Sum(nil))

	hashStr := hash[:8]
	hashInt64, err := strconv.ParseInt(hashStr, 16, 64)
	if err != nil {
		hashInt64 = 0
		for _, char := range player {
			hashInt64 = hashInt64*31 + int64(char)
		}
	}

	return int(hashInt64) % numServices
}

func syncAccountsConcurrent(t *singer.Tap, playerGroups []PlayerGroup, s *singer.State, c *Config) error {
	t.WriteSchemaFromStream(createAccountsStream())

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, group := range playerGroups {
		if len(group.Players) == 0 {
			continue
		}

		wg.Add(1)
		go func(players []string, service *RiotService) {
			defer wg.Done()

			for _, player := range players {
				acc, err := service.getAccount(player)
				if err != nil {
					mu.Lock()
					t.LogError("Failed to get account for player: " + player + " - skipping")
					mu.Unlock()
					continue
				}

				mu.Lock()
				t.WriteRecord(Accounts, acc)
				mu.Unlock()
			}
		}(group.Players, group.Service)
	}

	wg.Wait()
	t.WriteState(s)
	return nil
}

func syncElosConcurrent(t *singer.Tap, playerGroups []PlayerGroup, s *singer.State, c *Config) error {
	t.WriteSchemaFromStream(createEloStream())

	var wg sync.WaitGroup
	var mu sync.Mutex
	currentState := s

	for _, group := range playerGroups {
		if len(group.Players) == 0 {
			continue
		}

		wg.Add(1)
		go func(players []string, service *RiotService) {
			defer wg.Done()

			for _, player := range players {
				mu.Lock()
				from, k := currentState.Value[Elos][player]
				if !k {
					from = c.StartDate
				}
				mu.Unlock()

				fromTime, err := startDateAsTime(from)
				if err != nil {
					mu.Lock()
					t.LogError("Invalid start date: " + from + " for player: " + player + " - skipping")
					mu.Unlock()
					continue
				}

				if isToday(fromTime) {
					mu.Lock()
					t.Log(fmt.Sprintf("Already processed today for player %s, skipping", player))
					mu.Unlock()
					continue
				}

				elo, err := service.getElo(player)
				if err != nil {
					mu.Lock()
					t.LogError("Failed to get elo for player: " + player + " - skipping")
					mu.Unlock()
					continue
				}

				mu.Lock()
				t.WriteRecord(Elos, elo)
				if currentState.Value[Elos] == nil {
					currentState.Value[Elos] = make(map[string]string)
				}
				currentState.Value[Elos][player] = time.Now().Format("2006-01-02")
				t.WriteState(currentState)
				mu.Unlock()
			}
		}(group.Players, group.Service)
	}

	wg.Wait()
	return nil
}

func syncMatchesConcurrent(t *singer.Tap, playerGroups []PlayerGroup, s *singer.State, c *Config) error {
	t.WriteSchemaFromStream(createMatchesStream())

	var wg sync.WaitGroup
	var mu sync.Mutex
	currentState := s

	for groupIdx, group := range playerGroups {
		if len(group.Players) == 0 {
			continue
		}

		wg.Add(1)
		go func(players []string, service *RiotService, gIdx int) {
			defer wg.Done()

			for playerIdx, player := range players {
				mu.Lock()
				t.Log(fmt.Sprintf("Group %d: Starting player %d/%d: %s", gIdx+1, playerIdx+1, len(players), player))

				from, k := currentState.Value[Matches][player]
				if !k {
					from = c.StartDate
				}
				mu.Unlock()

				fromTime, err := startDateAsTime(from)
				if err != nil {
					mu.Lock()
					t.LogError("Invalid start date: " + from + " for player: " + player + " - skipping")
					mu.Unlock()
					continue
				}

				mu.Lock()
				t.Log(fmt.Sprintf("Processing matches from %s for player %s", from, player))
				mu.Unlock()

				ids, err := service.getMatchIdsByPlayer(player, fromTime, c.QueueId)
				if err != nil {
					mu.Lock()
					t.LogError("Failed to get match ids for player: " + player + " - skipping")
					mu.Unlock()
					continue
				}

				mu.Lock()
				t.Log(fmt.Sprintf("Found %d matches for player %s", len(ids), player))
				mu.Unlock()

				for i, id := range ids {
					match, err := service.getMatchDetails(id)
					if err != nil {
						mu.Lock()
						t.LogError("Failed to get match details for match id: " + id + " - skipping " + err.Error())
						mu.Unlock()
						continue
					}

					mWithId := MatchWithID{*match, match.Metadata.MatchID}
					mu.Lock()
					t.WriteRecord(Matches, mWithId)
					mu.Unlock()

					if (i+1)%50 == 0 {
						mu.Lock()
						t.Log(fmt.Sprintf("Progress: %d/%d matches processed for player %s", i+1, len(ids), player))
						mu.Unlock()
					}
				}

				mu.Lock()
				if currentState.Value[Matches] == nil {
					currentState.Value[Matches] = make(map[string]string)
				}
				currentState.Value[Matches][player] = time.Now().Format("2006-01-02")
				t.WriteState(currentState)
				mu.Unlock()
			}
		}(group.Players, group.Service, groupIdx)
	}

	wg.Wait()
	return nil
}

func syncMatchTimelinesConcurrent(t *singer.Tap, playerGroups []PlayerGroup, s *singer.State, c *Config) error {
	t.WriteSchemaFromStream(createMatchTimelineStream())

	var wg sync.WaitGroup
	var mu sync.Mutex
	currentState := s

	for groupIdx, group := range playerGroups {
		if len(group.Players) == 0 {
			continue
		}

		wg.Add(1)
		go func(players []string, service *RiotService, gIdx int) {
			defer wg.Done()

			for playerIdx, player := range players {
				mu.Lock()
				t.Log(fmt.Sprintf("Group %d: Starting player %d/%d: %s", gIdx+1, playerIdx+1, len(players), player))

				from, k := currentState.Value[MatchTimelines][player]
				if !k {
					from = c.StartDate
				}
				mu.Unlock()

				fromTime, err := startDateAsTime(from)
				if err != nil {
					mu.Lock()
					t.LogError("Invalid start date: " + from + " for player: " + player + " - skipping")
					mu.Unlock()
					continue
				}

				ids, err := service.getMatchIdsByPlayer(player, fromTime, c.QueueId)
				if err != nil {
					mu.Lock()
					t.LogError("Failed to get match ids for player: " + player + " - skipping")
					mu.Unlock()
					continue
				}

				mu.Lock()
				t.Log(fmt.Sprintf("Found %d matches for player %s", len(ids), player))
				mu.Unlock()

				for i, id := range ids {
					timeline, err := service.getMatchTimeline(id)
					if err != nil {
						mu.Lock()
						t.LogError("Failed to get match details for match id: " + id + " - skipping : " + err.Error())
						mu.Unlock()
						continue
					}

					mu.Lock()
					t.WriteRecord(MatchTimelines, timeline)
					mu.Unlock()

					if (i+1)%50 == 0 {
						mu.Lock()
						t.Log(fmt.Sprintf("Progress: %d/%d matches processed for player %s", i+1, len(ids), player))
						mu.Unlock()
					}
				}

				mu.Lock()
				if currentState.Value[MatchTimelines] == nil {
					currentState.Value[MatchTimelines] = make(map[string]string)
				}
				currentState.Value[MatchTimelines][player] = time.Now().Format("2006-01-02")
				t.WriteState(currentState)
				mu.Unlock()
			}
		}(group.Players, group.Service, groupIdx)
	}

	wg.Wait()
	return nil
}

func startDateAsTime(s string) (time.Time, error) {
	return time.Parse("2006-01-02", s)
}

func isToday(t time.Time) bool {
	now := time.Now()
	y1, m1, d1 := t.Date()
	y2, m2, d2 := now.Date()
	return y1 == y2 && m1 == m2 && d1 == d2
}
