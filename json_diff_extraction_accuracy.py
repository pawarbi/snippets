### implemented based on https://github.com/getomni-ai/benchmark/blob/main/src/evaluation/json.ts

from jsondiff import diff
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


def count_changes(diff_result: Any) -> DiffStats:
    """
    Count the number of additions, deletions, and modifications in a diff result.
    """
    changes: DiffStats = {
        'additions': 0,
        'deletions': 0,
        'modifications': 0,
        'total': 0
    }

    def traverse(obj: Any) -> None:
        if obj is None or not isinstance(obj, dict):
            return

        for key, value in obj.items():
            if isinstance(value, list):
                # Handle array diffs
                for operation, element in value:
                    if element is None or not isinstance(element, (dict, list)):
                        # Handle primitive value changes in arrays
                        if operation == '+':
                            changes['additions'] += 1
                        elif operation == '-':
                            changes['deletions'] += 1
                    else:
                        if operation == '+':
                            changes['additions'] += count_total_fields(element)
                        elif operation == '-':
                            changes['deletions'] += count_total_fields(element)
                        elif operation == '~':
                            # Handle array element modifications
                            traverse(element)
            else:
                if key.endswith('__deleted'):
                    if value is None or not isinstance(value, (dict, list)):
                        changes['deletions'] += 1
                    else:
                        changes['deletions'] += count_total_fields(value)
                elif key.endswith('__added'):
                    if value is None or not isinstance(value, (dict, list)):
                        changes['additions'] += 1
                    else:
                        changes['additions'] += count_total_fields(value)
                elif isinstance(value, dict) and value is not None:
                    if '__old' in value and '__new' in value:
                        if value['__old'] is None and value['__new'] is not None:
                            changes['modifications'] += count_total_fields(value['__new']) or 1
                        else:
                            changes['modifications'] += count_total_fields(value['__old']) or 1
                    else:
                        traverse(value)

    traverse(diff_result)

    changes['total'] = changes['additions'] + changes['deletions'] + changes['modifications']
    return changes


def calculate_json_accuracy(actual: Dict[str, Any], predicted: Dict[str, Any]) -> AccuracyResult:
    """
    Calculates accuracy for JSON structure and primitive values only.

    The accuracy is calculated as:
    1 - (number of differences / total fields in actual)

    Differences include:
    - Additions: Fields present in predicted but not in actual
    - Deletions: Fields present in actual but not in predicted
    - Modifications: Fields present in both but with different values

    A score of 1.0 means the JSONs are identical
    A score of 0.0 means completely different
    """
    # Get the diff result
    full_diff_result = diff(actual, predicted, syntax='explicit', marshal=True)
    diff_result = diff(actual, predicted, marshal=True)
    total_fields = count_total_fields(actual)

    if not diff_result:
        # If there's no diff, the JSONs are identical
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

    changes = count_changes(diff_result)
    score = max(
        0,
        1 - (changes['additions'] + changes['deletions'] + changes['modifications']) / total_fields
    )

    return {
        'score': round(score, 4),
        'json_diff': diff_result,
        'full_json_diff': full_diff_result,
        'json_diff_stats': changes,
        'total_fields': total_fields
    }


# Example usage
if __name__ == "__main__":
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
