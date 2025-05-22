import os
import glob
import json
import sys

tabulation = "   "

def load_config(config_file):
    """Load and validate the configuration file."""
    try:
        with open(config_file, 'r') as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"Error loading config file: {e}")
        sys.exit(1)

    required_keys = ["clients", "joiners", "nodes", "files"]
    for key in required_keys:
        if key not in cfg:
            print(f"Missing required key '{key}' in config file")
            sys.exit(1)

    # Validate files section
    for file_type, file_list in cfg["files"].items():
        if not isinstance(file_list, list):
            print(f"Error: '{file_type}' must be a list of files")
            sys.exit(1)
        if len(file_list) == 0:
            print(f"Error: '{file_type}' cannot be empty")
            sys.exit(1)

    return cfg

def get_expected_results_for_file(file_path, cfg):
    """Get the expected results for a specific file based on the config."""
    client_num = int(file_path.split('-')[-1].split('.')[0])
    
    review_file = cfg["files"]["reviews"][(client_num - 1) % len(cfg["files"]["reviews"])]
    movie_file = cfg["files"]["movies"][(client_num - 1) % len(cfg["files"]["movies"])]
    credits_file = cfg["files"]["credits"][(client_num - 1) % len(cfg["files"]["credits"])]

    # Results are only available for movies_metadata.csv and credits.csv
    if "movies_metadata.csv" not in movie_file:
        return None
    
    if "credits.csv" not in credits_file:
        return None
    
    # Return the appropriate expected results based on the review file
    if "ratings_small.csv" in review_file:
        return expected_reviews_small
    elif "ratings.csv" in review_file:
        return expected_reviews_big
    else:
        return None

def compare_results(actual_file_path, expected_results):
    """
    Compare actual query results with expected results.
    Skips queries that don't have expected results defined.
    
    Args:
        actual_file_path (str): Path to the file containing actual results
        expected_results (dict): Dictionary with expected results
            Format: {query_num: [expected_result1, expected_result2, ...]}
    
    Returns:
        dict: Dictionary with comparison results for each query
            Format: {query_num: {"matches": bool, "actual": list, "expected": list}}
        if there is no expected results, it returns None
    """
    if expected_results is None:
        return None

    # Read actual results
    with open(actual_file_path, 'r') as f:
        actual_lines = f.readlines()
    
    # Parse actual results into a dictionary
    actual_results = {}
    for line in actual_lines:
        if line.startswith('Query'):
            parts = line.split(': ', 1)
            query_num = int(parts[0].split()[1])
            # Split by comma but only if it's not inside brackets
            results = []
            current = ""
            in_brackets = False
            for char in parts[1].strip():
                if char == '[':
                    in_brackets = True
                elif char == ']':
                    in_brackets = False
                elif char == ',' and not in_brackets:
                    results.append(current.strip())
                    current = ""
                    continue
                current += char
            if current:
                results.append(current.strip())
            actual_results[query_num] = results
    
    # Compare results
    comparison = {}
    for query_num, actual in actual_results.items():
        # Skip if we don't have expected results for this query
        if query_num not in expected_results:
            continue
            
        expected = expected_results[query_num]
        
        # Compare sets to ignore order
        actual_set = set(actual)
        expected_set = set(expected)
        
        comparison[query_num] = {
            "matches": actual_set == expected_set,
            "actual": actual,
            "expected": expected,
            "missing": list(expected_set - actual_set),
            "extra": list(actual_set - expected_set)
        }
    
    return comparison

def print_comparison(comparison, file_name, cfg):
    """Print the comparison results in a readable format."""
    if not comparison:
        id = int(file_name.split('-')[-1].split('.')[0])
        movie_file = cfg["files"]["movies"][(id - 1) % len(cfg["files"]["movies"])]
        credits_file = cfg["files"]["credits"][(id - 1) % len(cfg["files"]["credits"])]
        reviews_file = cfg["files"]["reviews"][(id - 1) % len(cfg["files"]["reviews"])]

        print(f"\n{file_name}: ⏭️  Skipped - No expected results for")
        print(f"{tabulation}   • Movies:    {os.path.basename(movie_file)}")
        print(f"{tabulation}   • Credits:   {os.path.basename(credits_file)}")
        print(f"{tabulation}   • Reviews:   {os.path.basename(reviews_file)}\n")
        return True

    error_in_query = False

    for query_num, result in comparison.items():
        if not result["matches"]:
            error_in_query = True

    if not error_in_query:
        print(f"{file_name}: ✅")
        return True
    print(f"{file_name}: ❌")
    for query_num, result in comparison.items():
        print(f"{tabulation}Query {query_num}: ", end="")

        if result["matches"]:
            print(f"{tabulation}Results match: ✅")
        else:
            error_in_query = True
            print(f"{tabulation}Results don't match: ❌")
            if result["missing"]:
                print(f"{tabulation}Missing results:")
                for item in result["missing"]:
                    print(f"  - {item}")
            if result["extra"]:
                print(f"{tabulation}Extra results:")
                for item in result["extra"]:
                    print(f"  - {item}")

    if error_in_query:
        return False

# Expected results dictionaries
expected_reviews_big = {
    1: [
        "La Cienaga | Genres: [Comedy, Drama]",
        "Burnt Money | Genres: [Crime]",
        "The City of No Limits | Genres: [Thriller, Drama]",
        "Nicotina | Genres: [Drama, Action, Comedy, Thriller]",
        "Lost Embrace | Genres: [Drama, Foreign]",
        "Whisky | Genres: [Comedy, Drama, Foreign]",
        "The Holy Girl | Genres: [Drama, Foreign]",
        "The Aura | Genres: [Crime, Drama, Thriller]",
        "Bombón: The Dog | Genres: [Drama]",
        "Rolling Family | Genres: [Drama, Comedy]",
        "The Method | Genres: [Drama, Thriller]",
        "Every Stewardess Goes to Heaven | Genres: [Drama, Romance, Foreign]",
        "Tetro | Genres: [Drama, Mystery]",
        "The Secret in Their Eyes | Genres: [Crime, Drama, Mystery, Romance]",
        "Liverpool | Genres: [Drama]",
        "The Headless Woman | Genres: [Drama, Mystery, Thriller]",
        "The Last Summer of La Boyita | Genres: [Drama]",
        "The Appeared | Genres: [Horror, Thriller, Mystery]",
        "The Fish Child | Genres: [Drama, Thriller, Romance, Foreign]",
        "Cleopatra | Genres: [Drama, Comedy, Foreign]",
        "Roma | Genres: [Drama, Foreign]",
        "Conversations with Mother | Genres: [Comedy, Drama, Foreign]",
        "The Education of Fairies | Genres: [Drama]",
        "The Good Life | Genres: [Drama]"
    ],
    2: [
        "United States of America | Budget: 120153886644",
        "France | Budget: 2256831838",
        "United Kingdom | Budget: 1611604610",
        "India | Budget: 1169682797",
        "Japan | Budget: 832585873"
    ],
    3: [
        "Best Movie: ID: 125619 | Title: The forbidden education | Rating: 4.00 | Worst Movie: ID: 128598 | Title: Left for Dead | Rating: 1.00"
    ],
    4: [
        "Actor: Ricardo Darín | Appearances: 17",
        "Actor: Leonardo Sbaraglia | Appearances: 7",
        "Actor: Alejandro Awada | Appearances: 7",
        "Actor: Inés Efron | Appearances: 7",
        "Actor: Valeria Bertuccelli | Appearances: 7",
        "Actor: Pablo Echarri | Appearances: 6",
        "Actor: Rodrigo de la Serna | Appearances: 6",
        "Actor: Rafael Spregelburd | Appearances: 6",
        "Actor: Arturo Goetz | Appearances: 6",
        "Actor: Diego Peretti | Appearances: 6"
    ],
    5: [
        "Positive Avg Profit Ratio: 3587.09 | Negative Avg Profit Ratio: 25945.69"
    ]
}

expected_reviews_small = {
    1: [
        "La Cienaga | Genres: [Comedy, Drama]",
        "Burnt Money | Genres: [Crime]",
        "The City of No Limits | Genres: [Thriller, Drama]",
        "Nicotina | Genres: [Drama, Action, Comedy, Thriller]",
        "Lost Embrace | Genres: [Drama, Foreign]",
        "Whisky | Genres: [Comedy, Drama, Foreign]",
        "The Holy Girl | Genres: [Drama, Foreign]",
        "The Aura | Genres: [Crime, Drama, Thriller]",
        "Bombón: The Dog | Genres: [Drama]",
        "Rolling Family | Genres: [Drama, Comedy]",
        "The Method | Genres: [Drama, Thriller]",
        "Every Stewardess Goes to Heaven | Genres: [Drama, Romance, Foreign]",
        "Tetro | Genres: [Drama, Mystery]",
        "The Secret in Their Eyes | Genres: [Crime, Drama, Mystery, Romance]",
        "Liverpool | Genres: [Drama]",
        "The Headless Woman | Genres: [Drama, Mystery, Thriller]",
        "The Last Summer of La Boyita | Genres: [Drama]",
        "The Appeared | Genres: [Horror, Thriller, Mystery]",
        "The Fish Child | Genres: [Drama, Thriller, Romance, Foreign]",
        "Cleopatra | Genres: [Drama, Comedy, Foreign]",
        "Roma | Genres: [Drama, Foreign]",
        "Conversations with Mother | Genres: [Comedy, Drama, Foreign]",
        "The Education of Fairies | Genres: [Drama]",
        "The Good Life | Genres: [Drama]"
    ],
    2: [
        "United States of America | Budget: 120153886644",
        "France | Budget: 2256831838",
        "United Kingdom | Budget: 1611604610",
        "India | Budget: 1169682797",
        "Japan | Budget: 832585873"
    ],
    3: [
        "Best Movie: ID: 80717 | Title: Violeta Went to Heaven | Rating: 5.00 | Worst Movie: ID: 69278 | Title: Phase 7 | Rating: 2.75"
    ],
    4: [
        "Actor: Ricardo Darín | Appearances: 17",
        "Actor: Leonardo Sbaraglia | Appearances: 7",
        "Actor: Alejandro Awada | Appearances: 7",
        "Actor: Inés Efron | Appearances: 7",
        "Actor: Valeria Bertuccelli | Appearances: 7",
        "Actor: Pablo Echarri | Appearances: 6",
        "Actor: Rodrigo de la Serna | Appearances: 6",
        "Actor: Rafael Spregelburd | Appearances: 6",
        "Actor: Arturo Goetz | Appearances: 6",
        "Actor: Diego Peretti | Appearances: 6"
    ],
    5: [
        "Positive Avg Profit Ratio: 3587.09 | Negative Avg Profit Ratio: 25945.69"
    ]
}

def main():
    if len(sys.argv) != 2:
        print("Usage: python compare_outputs.py <config.json>")
        sys.exit(1)

    cfg = load_config(sys.argv[1])
    
    result_files = glob.glob("client-results/queries-results-*.txt")
    if not result_files:
        print("No result files found in client-results/")
        sys.exit(1)

    result_files.sort(key=lambda x: int(x.split('-')[-1].split('.')[0]))
    
    processed_files = set()
    all_passed = True

    for result_file in result_files:
        if result_file in processed_files:
            continue

        # Get the expected results based on the file and config
        expected_results = get_expected_results_for_file(result_file, cfg)
        
        comparison = compare_results(result_file, expected_results)

        if not print_comparison(comparison, result_file, cfg):
            all_passed = False

        processed_files.add(result_file)

    if all_passed:
        print("\nAll comparisons passed! ✅")
        sys.exit(0)
    else:
        print("\nSome comparisons failed! ❌")
        sys.exit(1)

if __name__ == "__main__":
    main()
