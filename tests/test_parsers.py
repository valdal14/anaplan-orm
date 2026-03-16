import pytest
from anaplan_orm.models import AnaplanModel
from anaplan_orm.parsers import CSVStringParser, JSONParser, XMLStringParser, SQLCursorParser
from unittest.mock import MagicMock


class EmployeeRoster(AnaplanModel):
    EmployeeID: int
    Department: str
    Salary: float


incoming_data = """
<AnaplanExport>
    <Row>
        <EmployeeID>101</EmployeeID>
        <Department>Sales</Department>
        <Salary>75000</Salary>
    </Row>
    <Row>
        <EmployeeID>102</EmployeeID>
        <Department>Engineering</Department>
        <Salary>90000</Salary>
    </Row>
</AnaplanExport>
"""

# NOTE: XMLStringParser tests ###################################################################


def test_xml_string_parser():
    # Arrange: Create an instance of the XMLStringParser
    xml_parser = XMLStringParser()
    # Act: From XML String create a list of AnaplanModel
    roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)
    # Assert: Verify the output is a list and it holds 2 elements
    assert isinstance(roster, list)
    assert len(roster) == 2


def test_to_csv_serialization():
    # Arrange: Parse the data
    xml_parser = XMLStringParser()
    roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)

    # Act: Serialization method
    csv_output = EmployeeRoster.to_csv(roster)

    # Assert: Verify the output is a string and contains the right CSV data
    assert isinstance(csv_output, str)

    # Check that the headers were dynamically extracted and formatted
    assert "EmployeeID,Department,Salary" in csv_output

    # Check that the floats and strings were properly serialized
    assert "101,Sales,75000.0" in csv_output
    assert "102,Engineering,90000.0" in csv_output


def test_to_csv_custom_separator():
    # Arrange
    xml_parser = XMLStringParser()
    roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)

    # Act: Pass a pipe character '|' as the separator
    csv_output = EmployeeRoster.to_csv(roster, separator="|")

    # Assert
    assert isinstance(csv_output, str)
    assert "EmployeeID|Department|Salary" in csv_output
    assert "101|Sales|75000.0" in csv_output


# NOTE: CSVStringParser tests ###################################################################
def test_csv_string_parser_success():
    """Test that the parser correctly maps CSV headers to dictionary keys."""
    csv_payload = "DEV_ID,DEV_NAME,DEV_AGE\n1001,Ada Love,35\n1002,Alan Turing,41"

    result = CSVStringParser.parse(csv_payload)

    assert len(result) == 2
    assert result[0] == {"DEV_ID": "1001", "DEV_NAME": "Ada Love", "DEV_AGE": "35"}
    assert result[1] == {"DEV_ID": "1002", "DEV_NAME": "Alan Turing", "DEV_AGE": "41"}


def test_csv_string_parser_invalid_type():
    """Test that the parser rejects non-string payloads."""
    with pytest.raises(TypeError) as exc_info:
        CSVStringParser.parse(["Not", "a", "string"])

    assert "Invalid Payload: Expected a string" in str(exc_info.value)


def test_csv_string_parser_empty_string():
    """Test that the parser rejects empty or whitespace-only strings."""
    with pytest.raises(ValueError) as exc_info:
        CSVStringParser.parse("   \n   ")

    assert "Cannot parse an empty CSV string" in str(exc_info.value)


# NOTE: JSONParser tests ########################################################################


def test_json_parser_flat_list():
    """Test that the parser handles a standard flat JSON array."""
    json_payload = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'

    result = JSONParser.parse(json_payload)

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["name"] == "Alice"


def test_json_parser_single_object():
    """Test that a single JSON object is correctly wrapped in a list."""
    json_payload = '{"id": 1, "name": "Alice"}'

    result = JSONParser.parse(json_payload)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_json_parser_with_data_key():
    """Test that the parser correctly extracts nested arrays using data_key."""
    json_payload = '{"status": "success", "data": [{"id": 1}, {"id": 2}]}'

    result = JSONParser.parse(json_payload, data_key="data")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[1]["id"] == 2


def test_json_parser_with_missing_data_key():
    """Test that providing a data_key that doesn't exist returns an empty list safely."""
    json_payload = '{"status": "success", "users": [{"id": 1}]}'

    result = JSONParser.parse(json_payload, data_key="wrong_key")

    assert isinstance(result, list)
    assert len(result) == 0


def test_json_parser_invalid_type():
    """Test that passing a non-string raises a TypeError."""
    with pytest.raises(TypeError) as exc_info:
        JSONParser.parse(["not", "a", "string"])

    assert "Expected a string" in str(exc_info.value)


def test_json_parser_empty_string():
    """Test that empty or whitespace strings raise a ValueError."""
    with pytest.raises(ValueError) as exc_info:
        JSONParser.parse("   ")

    assert "Cannot parse an empty JSON string" in str(exc_info.value)


def test_json_parser_invalid_json():
    """Test that malformed JSON strings are caught and raised gracefully."""
    bad_json = '{"id": 1, "name": "Alice" '  # Missing closing brace

    with pytest.raises(ValueError) as exc_info:
        JSONParser.parse(bad_json)

    assert "Failed to decode JSON payload" in str(exc_info.value)


def test_json_parser_data_key_on_list():
    """Test that asking for a data_key when the root is a list raises an error."""
    flat_list_json = '[{"id": 1}]'

    with pytest.raises(ValueError) as exc_info:
        JSONParser.parse(flat_list_json, data_key="data")

    assert "root is a list, not a dictionary" in str(exc_info.value)


def test_json_parser_invalid_return_type():
    """Test that JSON resolving to a primitive (like a string/bool) raises an error."""
    # "true" is technically valid JSON, but it evaluates to a boolean, not a dict/list
    bool_json = '"true"'

    with pytest.raises(TypeError) as exc_info:
        JSONParser.parse(bool_json)

    assert "must result in a dictionary or a list" in str(exc_info.value)


# NOTE: SQLCursorParser tests ###################################################################

def test_sql_cursor_parser_happy_path():
    """Test that the parser correctly zips cursor descriptions and fetched rows."""
    # Mock a standard Python DB-API 2.0 cursor
    mock_cursor = MagicMock()
    # description is a tuple of tuples: (column_name, type_code, display_size, etc...)
    mock_cursor.description = (("DEV_ID", None), ("DEV_NAME", None), ("DEV_AGE", None))
    mock_cursor.fetchall.return_value = [
        (1001, "Ada Lovelace", 36),
        (1002, "Grace Hopper", 85)
    ]
    
    # Parse the mock cursor
    result = SQLCursorParser.parse(mock_cursor)
    
    # Assert it created perfect dictionaries
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == {"DEV_ID": 1001, "DEV_NAME": "Ada Lovelace", "DEV_AGE": 36}
    assert result[1]["DEV_NAME"] == "Grace Hopper"

def test_sql_cursor_parser_invalid_object():
    """Test that passing a non-cursor object raises a TypeError."""
    # Passing the query string instead of the cursor
    bad_payload = "SELECT * FROM users"
    
    with pytest.raises(TypeError) as exc_info:
        SQLCursorParser.parse(bad_payload)
        
    assert "Expected a database cursor object" in str(exc_info.value)

def test_sql_cursor_parser_no_description():
    """Test that a cursor without a description (e.g., no query executed) raises an error."""
    mock_cursor = MagicMock()
    # Happens if you run an UPDATE/INSERT or haven't executed yet
    mock_cursor.description = None 
    
    with pytest.raises(ValueError) as exc_info:
        SQLCursorParser.parse(mock_cursor)
        
    assert "Cursor has no description" in str(exc_info.value)