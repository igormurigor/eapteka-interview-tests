import pytest
from etl_2 import check_csv_row_count

csv_file = "DE/csv/sales.csv"


def test_check_csv_row_count():
    expected_row_number = 100
    actual_row_number = check_csv_row_count(csv_file)
    check_csv_row_count(csv_file)
    assert actual_row_number == expected_row_number, f"Expected {expected_row_number}, but got {actual_row_number}"


# Run
if __name__ == '__main__':
    pytest.main()
