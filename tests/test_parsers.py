from unittest.mock import MagicMock

import pytest

from anaplan_orm.models import AnaplanModel
from anaplan_orm.parsers import CSVStringParser, JSONParser, SQLCursorParser, XMLStringParser


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


def test_xml_parser_xpath_flat_extraction():
    """Test that the parser handles simple, flat XML like the original parser did."""
    xml_payload = """
    <Export>
        <Row><ID>1001</ID><Name>Ada</Name></Row>
        <Row><ID>1002</ID><Name>Grace</Name></Row>
    </Export>
    """
    # No mapping provided, so it uses the legacy flat extraction
    result = XMLStringParser.parse(xml_payload, data_key=".//Row")

    assert len(result) == 2
    assert result[0] == {"ID": "1001", "Name": "Ada"}
    assert result[1]["Name"] == "Grace"


def test_xml_parser_xpath_deep_extraction():
    """Test that XPath correctly extracts both nested text and element attributes."""
    xml_payload = """
    <EnterpriseData>
        <EmployeeRecord region="EMEA">
            <Details empId="1001">
                <Profile><FullName>Ada Lovelace</FullName></Profile>
            </Details>
            <Office><City>London</City></Office>
        </EmployeeRecord>
    </EnterpriseData>
    """
    mapping = {
        "DEV_ID": "./Details/@empId",  # Attribute extraction!
        "DEV_NAME": "./Details/Profile/FullName",  # Nested text extraction!
        "DEV_LOCATION": "./Office/City",
        "REGION": "./@region",  # Root attribute extraction!
    }

    result = XMLStringParser.parse(xml_payload, data_key=".//EmployeeRecord", mapping=mapping)

    assert len(result) == 1
    assert result[0]["DEV_ID"] == "1001"
    assert result[0]["DEV_NAME"] == "Ada Lovelace"
    assert result[0]["DEV_LOCATION"] == "London"
    assert result[0]["REGION"] == "EMEA"


def test_xml_parser_invalid_xml():
    """Test that malformed XML safely raises a ValueError."""
    bad_xml = "<Export><Row>Unclosed tag</Export>"

    with pytest.raises(ValueError) as exc_info:
        XMLStringParser.parse(bad_xml)

    assert "Failed to decode XML payload" in str(exc_info.value)


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


def test_json_parser_jmespath_flat_extraction():
    """Test that JMESPath mapping works on a flat dictionary."""
    json_payload = '{"id": 1, "name": "Alice"}'
    mapping = {"DEV_ID": "id", "DEV_NAME": "name"}

    result = JSONParser.parse(json_payload, mapping=mapping)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == {"DEV_ID": 1, "DEV_NAME": "Alice"}


def test_json_parser_jmespath_deep_extraction():
    """Test that JMESPath correctly traverses deeply nested structures."""
    json_payload = """
    [
        {
            "employee": {"details": {"emp_id": 1001, "full_name": "Ada"}},
            "location": {"city": "London"}
        }
    ]
    """
    mapping = {
        "DEV_ID": "employee.details.emp_id",
        "DEV_NAME": "employee.details.full_name",
        "DEV_LOCATION": "location.city",
    }

    result = JSONParser.parse(json_payload, mapping=mapping)

    assert len(result) == 1
    assert result[0]["DEV_ID"] == 1001
    assert result[0]["DEV_NAME"] == "Ada"
    assert result[0]["DEV_LOCATION"] == "London"


def test_json_parser_jmespath_missing_keys():
    """Test that missing keys in the JSON return None gracefully without crashing."""
    json_payload = '[{"employee": {"details": {"emp_id": 1001}}}]'
    # "full_name" and "location" are missing from the payload
    mapping = {
        "DEV_ID": "employee.details.emp_id",
        "DEV_NAME": "employee.details.full_name",
        "DEV_LOCATION": "location.city",
    }

    result = JSONParser.parse(json_payload, mapping=mapping)

    assert result[0]["DEV_ID"] == 1001
    assert result[0]["DEV_NAME"] is None
    assert result[0]["DEV_LOCATION"] is None


# NOTE: SQLCursorParser tests ###################################################################


def test_sql_cursor_parser_happy_path():
    """Test that the parser correctly zips cursor descriptions and fetched rows."""
    # Mock a standard Python DB-API 2.0 cursor
    mock_cursor = MagicMock()
    # description is a tuple of tuples: (column_name, type_code, display_size, etc...)
    mock_cursor.description = (("DEV_ID", None), ("DEV_NAME", None), ("DEV_AGE", None))
    mock_cursor.fetchall.return_value = [(1001, "Ada Lovelace", 36), (1002, "Grace Hopper", 85)]

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
