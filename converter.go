package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Message struct {
	User           string     `json:"user"`
	Text           string     `json:"text"`
	GivenReactions []Reaction `json:"reactions,omitempty"`
	Timestamp      string     `json:"ts"`
}

type Reaction struct {
	Name  string   `json:"name"`
	Users []string `json:"users"`
	Count int      `json:"count"`
}

type User struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Profile      Profile
	IsRestricted bool `json:"is_restricted"`
	Deleted      bool `json:"deleted"`
}

type Profile struct {
	DisplayName string `json:"display_name"`
}

type Stats struct {
	UserID                string
	Name                  string
	DisplayName           string
	Posts                 int
	GivenReactions        int
	GivenReactionUser     map[string]bool
	ReceivedReactions     int
	ReceivedReactionUsers map[string]bool
	IsRestricted          bool
	Deleted               bool
}

type StatsByUser map[string]*Stats
type StatsByDay map[string]StatsByUser
type StatsByChannel map[string]StatsByDay

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Error: No directory path specified.")
		return
	} else if len(os.Args) > 2 {
		fmt.Println("Error: Too many arguments. The correct usage is `go run converter.go PATH`.")
		return
	}

	basePath := os.Args[1]
	statsByChannel := make(StatsByChannel)

	// Load names
	users, err := loadUsers(basePath + "/users.json")
	if err != nil {
		fmt.Println("Error loading users:", err)
		return
	}

	err = filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".json" {
			dir := filepath.Dir(path)
			if filepath.Base(dir) == filepath.Base(basePath) {
				// Skip JSON files that are not
				// in a channel folder
				return nil
			}

			messages, err := readMessagesFromJSONFile(path)
			if err != nil {
				return err
			}

			updateStats(statsByChannel, filepath.Base(dir), messages, users)
		}

		return nil
	})

	if err != nil {
		fmt.Println("Error processing files:", err)
		return
	}

	outputName := "./"+strings.Replace(strings.Replace(basePath, ".", "", -1), "/", "", -1)+".csv"
	exportCSV(outputName, statsByChannel)
	fmt.Println(outputName, " file created successfully.")
}

func loadUsers(usersFile string) (map[string]*User, error) {
	data, err := ioutil.ReadFile(usersFile)
	if err != nil {
		return nil, err
	}

	var users []User
	err = json.Unmarshal(data, &users)
	if err != nil {
		return nil, err
	}

	userMap := make(map[string]*User)
	for _, user := range users {
		userMap[user.ID] = &User{
			ID:           user.ID,
			Profile:      user.Profile,
			IsRestricted: user.IsRestricted,
			Deleted:      user.Deleted,
		}
	}

	return userMap, nil
}

func readMessagesFromJSONFile(filePath string) ([]Message, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var messages []Message
	err = json.Unmarshal(data, &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func updateStats(statsByChannel StatsByChannel, channelName string, messages []Message, users map[string]*User) {

	ud, ok := statsByChannel[channelName]
	if !ok {
		ud = make(StatsByDay)
		statsByChannel[channelName] = ud
	}

	for _, message := range messages {
		// Format time.Time value as "YYYY/MM/DD"
		if users == nil {
			continue
		}

		if len(message.Timestamp) == 0 {
			continue
		}

		floatTs, err := strconv.ParseFloat(message.Timestamp, 64)
		if err != nil {
			fmt.Println("Error parsing timestamp:", err)
			return
		}
		formattedTime := time.Unix(int64(floatTs), 0).Format("2006-01-02")
		statsByUser, ok := ud[formattedTime]
		if !ok {
			statsByUser = make(StatsByUser)
			ud[formattedTime] = statsByUser
		}

		stats, ok := statsByUser[message.User]
		if !ok {
			u := users[message.User]
			if u == nil {
				continue
			}
			stats = &Stats{
				UserID: u.ID,
				Name:         u.Name,
				DisplayName:  strings.ReplaceAll(u.Profile.DisplayName, ",", " "),
				IsRestricted: u.IsRestricted,
				Deleted:      u.Deleted,
			}
			statsByUser[message.User] = stats
		}

		stats.Posts++

		for _, reaction := range message.GivenReactions {
			for _, reactingUser := range reaction.Users {
				reactingStats, ok := statsByUser[reactingUser]
				if !ok {
					u := users[reactingUser]
					if u == nil {
						continue
					}
					reactingStats = &Stats{
						UserID:       u.ID,
						Name:         u.Name,
						DisplayName:  strings.ReplaceAll(u.Profile.DisplayName, ",", " "),
						IsRestricted: u.IsRestricted,
						Deleted:      u.Deleted,
					}
					statsByUser[reactingUser] = reactingStats
				}

				reactingStats.ReceivedReactions++
				if reactingStats.ReceivedReactionUsers == nil {
					reactingStats.ReceivedReactionUsers = make(map[string]bool)
				}
				reactingStats.ReceivedReactionUsers[message.User] = true

				stats.GivenReactions++
				if stats.GivenReactionUser == nil {
					stats.GivenReactionUser = make(map[string]bool)
				}
				stats.GivenReactionUser[reactingUser] = true
			}
		}
	}
}

func exportCSV(fileName string, statsByChannel StatsByChannel) error {
	// Create a new CSV file
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header to CSV
	header := []string{
		"display_name",
		"name",
		"is_restricted",
		"deleted",
		"day",
		"posts",
		"received_reations",
		"received_reaction_users",
		"given_reactions",
		"given_reation_users",
		"channel_name",
	}
	err = writer.Write(header)
	if err != nil {
		return err
	}

	// Write data to CSV
	for channelName, ud := range statsByChannel {
		for day, us := range ud {
			for _, s := range us {
				row := []string{
					s.DisplayName,
					s.Name,
					strconv.FormatBool(s.IsRestricted),
					strconv.FormatBool(s.Deleted),
					day,
					strconv.Itoa(s.Posts),
					strconv.Itoa(s.GivenReactions),
					strconv.Itoa(len(s.GivenReactionUser)),
					strconv.Itoa(s.ReceivedReactions),
					strconv.Itoa(len(s.ReceivedReactionUsers)),
					channelName,
				}
				err := writer.Write(row)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
