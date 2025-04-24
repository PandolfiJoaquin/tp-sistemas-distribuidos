def compare_results(actual_file_path, expected_results):
    """
    Compare actual query results with expected results.
    
    Args:
        actual_file_path (str): Path to the file containing actual results
        expected_results (dict): Dictionary with expected results
            Format: {query_num: [expected_result1, expected_result2, ...]}
    
    Returns:
        dict: Dictionary with comparison results for each query
            Format: {query_num: {"matches": bool, "actual": list, "expected": list}}
    """
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
    for query_num, expected in expected_results.items():
        actual = actual_results.get(query_num, [])
        
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

def print_comparison(comparison):
    """Print the comparison results in a readable format."""
    for query_num, result in comparison.items():
        print(f"Query {query_num}:")
        if query_num == 5:
            print("For query 5 there is no expected results, got:")
            for item in result["actual"]:
                print(f"  - {item}")
            continue
            
        if result["matches"]:
            print("✅ Results match!")
        else:
            print("❌ Results don't match")
            if result["missing"]:
                print("Missing results:")
                for item in result["missing"]:
                    print(f"  - {item}")
            if result["extra"]:
                print("Extra results:")
                for item in result["extra"]:
                    print(f"  - {item}")

# Example usage:
if __name__ == "__main__":
    # Expected results exactly matching the current file
    expected = {
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
        5: []
    }
    
    file_path = "client-results/queries-results-1.txt"
    # Compare results
    comparison = compare_results(file_path, expected)
    print_comparison(comparison)
