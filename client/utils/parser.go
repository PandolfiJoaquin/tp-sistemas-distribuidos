package utils

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var hexEscape = regexp.MustCompile(`\\x([0-9A-Fa-f]{2})`)

func isEscaped(s string, i int) bool {
	count := 0
	for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
		count++
	}
	return count%2 == 1
}

// Splits a string into pairs based on commas, while respecting quoted sections.
func splitPairs(input string) ([]string, error) {
	var pairs []string
	var current strings.Builder
	inQuote := false
	var quoteChar rune // will hold either ' or "

	for i, r := range input {
		// Check if the current rune is a quote (either ' or ")
		if (r == '\'' || r == '"') && !isEscaped(input, i) {
			if inQuote {
				// We're inside a quoted section; only toggle off if we see the same kind of quote.
				if r == quoteChar {
					inQuote = false
					quoteChar = 0
				}
			} else {
				// Start a quoted section.
				inQuote = true
				quoteChar = r
			}
			current.WriteRune(r)
			continue
		}

		if r == ',' && !inQuote {
			// Outside any quotes, treat comma as a separator.
			pair := strings.TrimSpace(current.String())
			if pair != "" {
				pairs = append(pairs, pair)
			}
			current.Reset()
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		pair := strings.TrimSpace(current.String())
		if pair != "" {
			pairs = append(pairs, pair)
		}
	}

	return pairs, nil
}

// FixJSONObject fixes a JSON object by ensuring it is properly formatted. The "bad" format comes from python dictionaries. Thus we can take some liberties when formatting.
func FixJSONObject(input string) (string, error) {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '{' || input[len(input)-1] != '}' {
		return "", fmt.Errorf("input is not a valid JSON object: %q", input)
	}
	input = hexEscape.ReplaceAllString(input, `\u00$1`)
	content := strings.TrimSpace(input[1 : len(input)-1])
	if content == "" {
		return "{}", nil
	}

	//fmt.Printf("raw json: %v\n", content)

	pairs, err := splitPairs(content)
	if err != nil {
		return "", fmt.Errorf("error splitting pairs: %v", err)
	}
	//fmt.Printf("pairs: %v\n", pairs)

	var entries []string
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("unable to parse pair: %q", pair)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		key = strings.Trim(key, "'")
		keyQuoted := strconv.Quote(key)

		var valueFixed string
		if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
			inner := value[1 : len(value)-1]
			//inner = hexEscape.ReplaceAllString(inner, `\u00$1`)
			valueFixed = strconv.Quote(inner)
		} else if strings.ToLower(value) == "none" {
			valueFixed = "null"
		} else {
			valueFixed = value
		}

		entries = append(entries, fmt.Sprintf("%s: %s", keyQuoted, valueFixed))
	}

	goodJSON := "{" + strings.Join(entries, ", ") + "}"
	//fmt.Printf("fixed json: %s\n", goodJSON)
	return goodJSON, nil
}

// Fixes a JSON array by ensuring it is properly formatted. See FixJSONObject for details.
func fixJSONArray(input string) (string, error) {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '[' || input[len(input)-1] != ']' {
		return "", fmt.Errorf("input is not a valid JSON array: %q", input)
	}

	// Use regex to extract substrings matching { ... }
	re := regexp.MustCompile(`\{[^}]*\}`)
	matches := re.FindAllString(input, -1)
	if len(matches) == 0 {
		return "", fmt.Errorf("no objects found in input: %q", input)
	}

	var fixedObjects []string
	for _, obj := range matches {
		fixed, err := FixJSONObject(obj)
		if err != nil {
			return "", fmt.Errorf("error fixing object %q: %v", obj, err)
		}
		fixedObjects = append(fixedObjects, fixed)
	}

	// Reassemble the objects into a proper JSON array.
	res := "[" + strings.Join(fixedObjects, ", ") + "]"
	//fmt.Printf("fixed json array: %s\n", res)
	return res, nil
}

// ParseJSONToObject parses a JSON object into a struct of type T.
func ParseJSONToObject[T any](input string) (*T, error) {
	if input == "" {
		return nil, nil
	}

	fixed, err := FixJSONObject(input)
	if err != nil {
		return nil, fmt.Errorf("error fixing JSON object: %v", err)
	}

	var result T
	err = json.Unmarshal([]byte(fixed), &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}
	return &result, nil
}

// ParseJSONArray parses a JSON array of objects into a slice of type T.
func ParseJSONArray[T any](input string) ([]T, error) {
	if input == "" {
		return nil, nil
	}

	if input == "[]" {
		return []T{}, nil
	}

	fixed, err := fixJSONArray(input)
	if err != nil {
		return nil, fmt.Errorf("error fixing JSON array: %v", err)
	}

	var result []T
	err = json.Unmarshal([]byte(fixed), &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	return result, nil
}

// parseBool converts a string to bool ("True"/"False")
func parseBool(str, field string) (bool, error) {
	// Uses strings.EqualFold to handle case insensitivity.
	if strings.EqualFold(str, "true") {
		return true, nil
	} else if strings.EqualFold(str, "false") {
		return false, nil
	}

	return false, fmt.Errorf("invalid %s: %q", field, str)
}

func parseUint8(str, field string) (uint8, error) {
	if str == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(str, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return uint8(v), nil
}

// parseUint32 converts a string to uint32
func parseUint32(str, field string) (uint32, error) {
	if str == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(str, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return uint32(v), nil
}

// parseUint64 converts a string to uint64
func parseUint64(str, field string) (uint64, error) {
	if str == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return v, nil
}

// parseFloat32 converts a string to float32
func parseFloat32(str, field string) (float32, error) {
	if str == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(str, 32)
	if err != nil {
		return 0, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return float32(v), nil
}

// parseTime converts a string to time.Time
func parseTime(str, field string) (time.Time, error) {
	if str == "" {
		return time.Time{}, nil
	}
	v, err := time.Parse("2006-01-02", str)
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing %s: %v", field, err)
	}
	return v, nil
}

func parseTimestamp(str, field string) (time.Time, error) {
	if str == "" {
		return time.Time{}, nil
	}

	// Convert the string to an int64 timestamp
	timestamp, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing %s: %v", field, err)
	}

	// Parse the timestamp into a time.Time object
	t := time.Unix(timestamp, 0)

	return t, nil
}

// hasNaNValues checks whether required fields are empty or NaN
func hasNaNValues(record []string, notNa []int) bool {
	for _, idx := range notNa {
		value := record[idx]
		if value == "" || value == "NaN" {
			return true
		}
	}
	return false
}
