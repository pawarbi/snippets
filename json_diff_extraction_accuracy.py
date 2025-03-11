### implemented based on https://github.com/getomni-ai/benchmark/blob/main/src/evaluation/json.ts
from deepdiff import DeepDiff
from typing import Dict, Any, TypedDict, Optional


class DiffStats(TypedDict):
    additions: int
    deletions: int
    modifications: int
    total: int


class AccuracyResult(TypedDict):
    score: float
    full_json_diff: Dict[str, Any]
    json_diff: Dict[str, Any]
    json_diff_stats: Optional[DiffStats]
    total_fields: int


def count_total_fields(obj: Any) -> int:
    """
    Count the total number of primitive fields in a JSON object.
    """
    count = 0

    def traverse(current: Any) -> None:
        nonlocal count
        if current is None or not isinstance(current, (dict, list)):
            return

        if isinstance(current, list):
            # Traverse into array elements if they're objects
            for item in current:
                if isinstance(item, (dict, list)):
                    traverse(item)
                else:
                    count += 1
        else:
            for key in current:
                # Skip diff metadata keys
                if '__' in key:
                    continue

                # Only count primitive value fields
                if (current[key] is None or
                        isinstance(current[key], (str, int, float, bool))):
                    count += 1
                # Recurse into nested objects and arrays
                elif isinstance(current[key], (dict, list)):
                    traverse(current[key])

    traverse(obj)
    return count


def count_changes_deepdiff(diff_result: Dict[str, Any]) -> DiffStats:
    """
    Count the number of additions, deletions, and modifications from a DeepDiff result.
    """
    changes = {
        'additions': 0,
        'deletions': 0,
        'modifications': 0,
        'total': 0
    }
    
    # Count additions (dictionary_item_added and iterable_item_added)
    if 'dictionary_item_added' in diff_result:
        # Each addition might be a primitive or a complex structure
        for _, value in diff_result['dictionary_item_added'].items():
            if value is None or not isinstance(value, (dict, list)):
                changes['additions'] += 1
            else:
                changes['additions'] += count_total_fields(value)
    
    if 'iterable_item_added' in diff_result:
        for _, value in diff_result['iterable_item_added'].items():
            if value is None or not isinstance(value, (dict, list)):
                changes['additions'] += 1
            else:
                changes['additions'] += count_total_fields(value)
    
    # Count deletions (dictionary_item_removed and iterable_item_removed)
    if 'dictionary_item_removed' in diff_result:
        for _, value in diff_result['dictionary_item_removed'].items():
            if value is None or not isinstance(value, (dict, list)):
                changes['deletions'] += 1
            else:
                changes['deletions'] += count_total_fields(value)
    
    if 'iterable_item_removed' in diff_result:
        for _, value in diff_result['iterable_item_removed'].items():
            if value is None or not isinstance(value, (dict, list)):
                changes['deletions'] += 1
            else:
                changes['deletions'] += count_total_fields(value)
    
    # Count modifications (values_changed and type_changes)
    if 'values_changed' in diff_result:
        changes['modifications'] += len(diff_result['values_changed'])
    
    if 'type_changes' in diff_result:
        changes['modifications'] += len(diff_result['type_changes'])
    
    changes['total'] = changes['additions'] + changes['deletions'] + changes['modifications']
    return changes


def calculate_json_accuracy(actual: Dict[str, Any], predicted: Dict[str, Any]) -> AccuracyResult:
    """
    Calculates accuracy for JSON structure and primitive values.
    
    The accuracy is calculated as:
    1 - (number of differences / total fields in actual)
    
    Differences include:
    - Additions: Fields present in predicted but not in actual
    - Deletions: Fields present in actual but not in predicted
    - Modifications: Fields present in both but with different values
    
    A score of 1.0 means the JSONs are identical
    A score of 0.0 means completely different
    """
    # Count the total fields
    total_fields = count_total_fields(actual)
    
    # Get the DeepDiff result
    diff_result = DeepDiff(actual, predicted, verbose_level=2)
    
    # If there's no diff, the JSONs are identical
    if not diff_result:
        return {
            'score': 1.0,
            'json_diff': {},
            'full_json_diff': {},
            'json_diff_stats': {
                'additions': 0,
                'deletions': 0,
                'modifications': 0,
                'total': 0
            },
            'total_fields': total_fields
        }
    
    # Count the changes
    changes = count_changes_deepdiff(diff_result)
    
    # Calculate the score
    score = max(0, 1 - (changes['total'] / total_fields if total_fields > 0 else 0))
    
    return {
        'score': round(score, 4),
        'json_diff': diff_result,
        'full_json_diff': diff_result,  # Same as json_diff for DeepDiff
        'json_diff_stats': changes,
        'total_fields': total_fields
    }


# Example usage
if __name__ == "__main__":
    # Install deepdiff if needed: pip install deepdiff
    
    actual = {
        "name": "John",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "New York"
        },
        "hobbies": ["reading", "swimming"]
    }
    
    predicted = {
        "name": "John",
        "age": 31,  # Changed
        "address": {
            "street": "123 Main St",
            "city": "Boston"  # Changed
        },
        "hobbies": ["reading", "cycling"],  # Changed swimming to cycling
        "occupation": "engineer"  # Added
    }
    
    result = calculate_json_accuracy(actual, predicted)
    print(f"Accuracy score: {result['score']}")
    print(f"Total fields: {result['total_fields']}")
    print(f"Diff stats: {result['json_diff_stats']}")
