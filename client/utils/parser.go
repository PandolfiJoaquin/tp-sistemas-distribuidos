package utils

import (
	"fmt"
	"strconv"
	"strings"
	"encoding/json"
	"regexp"
)

func splitPairs(input string) ([]string, error) {
	var pairs []string
	var current strings.Builder
	inQuote := false
	var quoteChar rune // will hold either ' or "

	for _, r := range input {
		// Check if the current rune is a quote (either ' or ")
		if r == '\'' || r == '"' {
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

func fixJSONObject(input string) (string, error) {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '{' || input[len(input)-1] != '}' {
		return "", fmt.Errorf("input is not a valid JSON object: %q", input)
	}
	content := strings.TrimSpace(input[1 : len(input)-1])
	if content == "" {
		return "{}", nil
	}

	pairs, err := splitPairs(content)
	if err != nil {
		return "", fmt.Errorf("error splitting pairs: %v", err)
	}

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
			valueFixed = strconv.Quote(inner)
		} else if strings.ToLower(value) == "none" {
			valueFixed = "null"
		} else {
			valueFixed = value
		}

		entries = append(entries, fmt.Sprintf("%s: %s", keyQuoted, valueFixed))
	}

	goodJSON := "{" + strings.Join(entries, ", ") + "}"
	return goodJSON, nil
}

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
		fixed, err := fixJSONObject(obj)
		if err != nil {
			return "", fmt.Errorf("error fixing object %q: %v", obj, err)
		}
		fixedObjects = append(fixedObjects, fixed)
	}

	// Reassemble the objects into a proper JSON array.
	return "[" + strings.Join(fixedObjects, ", ") + "]", nil
}

func ParseObject[T any] (input string) (*T, error){
	if input == ""{
		return nil, nil
	}

	fixed , err := fixJSONObject(input)
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

func ParseObjectArray[T any] (input string) ([]T, error){
	if input == ""{
		return nil, nil
	}

	if input == "[]"{
		return []T{}, nil
	}

	fixed , err := fixJSONArray(input)
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