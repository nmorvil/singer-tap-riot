package tap

import (
	"github.com/KnutZuidema/golio/riot/account"
	"github.com/KnutZuidema/golio/riot/lol"
	"github.com/invopop/jsonschema"
	"github.com/wk8/go-ordered-map/v2"
	"singer-tap-riot/pkg/singer"
)

type Props = orderedmap.OrderedMap[string, *jsonschema.Schema]

func createMatchesStream() singer.Stream {
	reflector := jsonschema.Reflector{
		DoNotReference: true,
	}
	return singer.Stream{
		TapStreamID: Matches,
		Stream:      Matches,
		Schema:      reflector.Reflect(new(MatchWithID)),
		Metadata: []singer.StreamMetadata{
			{
				Breadcrumb: []string{},
				Metadata: map[string]interface{}{
					"inclusion":      "available",
					"key-properties": []string{"matchId"},
				},
			},
		},
	}
}

func createMatchTimelineStream() singer.Stream {
	reflector := jsonschema.Reflector{
		DoNotReference: true,
	}
	return singer.Stream{
		TapStreamID: MatchTimelines,
		Stream:      MatchTimelines,
		Schema:      reflector.Reflect(new(MatchTimeline)),
		Metadata: []singer.StreamMetadata{
			{
				Breadcrumb: []string{},
				Metadata: map[string]interface{}{
					"inclusion":      "available",
					"key-properties": []string{"matchId"},
				},
			},
		},
	}
}

func createEloStream() singer.Stream {
	reflector := jsonschema.Reflector{
		DoNotReference: true,
	}

	return singer.Stream{
		TapStreamID: Elos,
		Stream:      Elos,
		Schema:      reflector.Reflect(new(Elo)),
		Metadata: []singer.StreamMetadata{
			{
				Breadcrumb: []string{},
				Metadata: map[string]interface{}{
					"inclusion":      "available",
					"key-properties": []string{"puuid", "date"},
				},
			},
		},
	}
}

func createAccountsStream() singer.Stream {
	reflector := jsonschema.Reflector{
		DoNotReference: true,
	}
	return singer.Stream{
		TapStreamID: Accounts,
		Stream:      Accounts,
		Schema:      reflector.Reflect(new(account.Account)),
		Metadata: []singer.StreamMetadata{
			{
				Breadcrumb: []string{},
				Metadata: map[string]interface{}{
					"inclusion":      "available",
					"key-properties": []string{"puuid"},
				},
			},
		},
	}
}

type MatchWithID struct {
	lol.Match
	MatchID string `json:"matchId"`
}

type Elo struct {
	Puuid        string `json:"puuid"`
	Date         string `json:"date"`
	LeaguePoints int    `json:"leaguePoints"`
	Tier         string `json:"tier"`
	Rank         string `json:"rank"`
}

type MatchTimeline struct {
	Frames  []MatchFrame     `json:"frames"`
	Events  []lol.MatchEvent `json:"events"`
	MatchId string           `json:"matchId"`
}

type MatchFrame struct {
	CurrentGold              float64     `json:"currentGold"`
	DamageStats              DamageStats `json:"damageStats"`
	GoldPerSecond            float64     `json:"goldPerSecond"`
	JungleMinionsKilled      float64     `json:"jungleMinionsKilled"`
	Level                    float64     `json:"level"`
	MinionsKilled            float64     `json:"minionsKilled"`
	PlayerID                 string      `json:"playerId"`
	Position                 Position    `json:"position"`
	TimeEnemySpentControlled float64     `json:"timeEnemySpentControlled"`
	TotalGold                float64     `json:"totalGold"`
	XP                       float64     `json:"xp"`
	Timestamp                float64     `json:"timestamp"`
}

type DamageStats struct {
	MagicDamageDone               float64 `json:"magicDamageDone"`
	MagicDamageDoneToChampions    float64 `json:"magicDamageDoneToChampions"`
	MagicDamageTaken              float64 `json:"magicDamageTaken"`
	PhysicalDamageDone            float64 `json:"physicalDamageDone"`
	PhysicalDamageDoneToChampions float64 `json:"physicalDamageDoneToChampions"`
	PhysicalDamageTaken           float64 `json:"physicalDamageTaken"`
	TotalDamageDone               float64 `json:"totalDamageDone"`
	TotalDamageDoneToChampions    float64 `json:"totalDamageDoneToChampions"`
	TotalDamageTaken              float64 `json:"totalDamageTaken"`
	TrueDamageDone                float64 `json:"trueDamageDone"`
	TrueDamageDoneToChampions     float64 `json:"trueDamageDoneToChampions"`
	TrueDamageTaken               float64 `json:"trueDamageTaken"`
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}
