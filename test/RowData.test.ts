import { PostgresConnection } from "../src/postgres";
import { PGConnectionPool } from "../src/postgres";
import { describe, test, expect, beforeEach, afterEach, assert } from "vitest";
import { mock, mockClear, MockProxy } from "vitest-mock-extended";
import { ColumnNotFoundError, ColumnTypeError } from "../src/tsdbc";

describe("RowData", () => {
  let connectionPool: MockProxy<PGConnectionPool>;
  let connection: PostgresConnection;

  beforeEach(() => {
    connectionPool = mock<PGConnectionPool>();
    connection = new PostgresConnection(connectionPool);
  });

  afterEach(() => {
    mockClear(connectionPool);
  });

  test.each([
    {
      rows: [{ username: "Warlord5417" }],
      columnLabel: "username",
      expectedValue: "Warlord5417",
    },
    {
      rows: [{ password: "super-secret-password" }],
      columnLabel: "password",
      expectedValue: "super-secret-password",
    },
    {
      rows: [{ lastName: "Parkinson" }],
      columnLabel: "lastName",
      expectedValue: "Parkinson",
    },
    {
      rows: [{ middleName: null }],
      columnLabel: "middleName",
      expectedValue: null,
    },
    {
      rows: [{ dob: "2004-12-13" }],
      columnLabel: "dob",
      expectedValue: "2004-12-13",
    },
    {
      rows: [{ email: null }],
      columnLabel: "email",
      expectedValue: null,
    },
    {
      rows: [{ lrn: null }],
      columnLabel: "lrn",
      expectedValue: null,
    },
  ])(
    "RowData.getString($columnLabel) -> value is string or null -> should return $expectedValue",
    async ({ rows, columnLabel, expectedValue }) => {
      connectionPool.query.mockResolvedValue({ rows });

      const statement = connection.createStatement("SELECT username FROM users");
      const rowData = await statement.execute();

      expect(rowData.at(0)?.getString(columnLabel)).toBe(expectedValue);
    }
  );

  test.each([
    {
      columnLabel: "middleName",
    },
    {
      columnLabel: "isBad",
    },
    {
      columnLabel: "isHandsome",
    },
  ])(
    "RowData.getString($columnLabel) -> value is undefined -> should throw Error",
    async ({ columnLabel }) => {
      connectionPool.query.mockResolvedValue({ rows: [{}] });
      const errorMessage = `Column '${columnLabel}' was not found`;

      const statement = connection.createStatement("SELECT * FROM table");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getString(columnLabel);

      assert.throws(action, ColumnNotFoundError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ height: 6 }],
      type: "number",
      columnLabel: "height",
    },
    {
      rows: [{ isAdmin: true }],
      type: "boolean",
      columnLabel: "isAdmin",
    },
    {
      rows: [{ dob: new Date("2004-12-13") }],
      type: "Date",
      columnLabel: "dob",
    },
    {
      rows: [{ obj: { something: "idk" } }],
      type: "unknown",
      columnLabel: "obj",
    },
  ])(
    "RowData.getString($columnLabel) -> value is $type -> should throw ColumnTypeError",
    async ({ rows, type, columnLabel }) => {
      connectionPool.query.mockResolvedValue({ rows });
      const errorMessage = `Expected column '${columnLabel}' to be a 'string', received '${type}' instead`;

      const statement = connection.createStatement("FOO");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getString(columnLabel);

      assert.throws(action, ColumnTypeError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ total_cost: 46.99 }],
      columnLabel: "total_cost",
      expectedValue: 46.99,
    },
    {
      rows: [{ students: null }],
      columnLabel: "students",
      expectedValue: null,
    },
    {
      rows: [{ id: 79 }],
      columnLabel: "id",
      expectedValue: 79,
    },
    {
      rows: [{ change: 0.99 }],
      columnLabel: "change",
      expectedValue: 0.99,
    },
    {
      rows: [{ age: null }],
      columnLabel: "age",
      expectedValue: null,
    },
    {
      rows: [{ role_no: null }],
      columnLabel: "role_no",
      expectedValue: null,
    },
  ])(
    "RowData.getNumber($columnLabel) -> value is number or null -> should return $expectedValue",
    async ({ rows, columnLabel, expectedValue }) => {
      connectionPool.query.mockResolvedValue({ rows });

      const statement = connection.createStatement("SELECT * FROM table");
      const rowData = await statement.execute();

      expect(rowData.at(0)?.getNumber(columnLabel)).toBe(expectedValue);
    }
  );

  test.each([
    {
      columnLabel: "isCreep",
    },
    {
      columnLabel: "isGood",
    },
    {
      columnLabel: "isDead",
    },
  ])(
    "RowData.getNumber($columnLabel) -> value is undefined -> should throw a ColumnNotFoundError",
    async ({ columnLabel }) => {
      connectionPool.query.mockResolvedValue({ rows: [{}] });
      const errorMessage = `Column '${columnLabel}' was not found`;

      const statement = connection.createStatement("SELECT * FROM table");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getNumber(columnLabel);

      assert.throws(action, ColumnNotFoundError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ name: "Jaypee" }],
      type: "string",
      columnLabel: "name",
    },
    {
      rows: [{ isAdmin: true }],
      type: "boolean",
      columnLabel: "isAdmin",
    },
    {
      rows: [{ dob: new Date("2004-12-13") }],
      type: "Date",
      columnLabel: "dob",
    },
  ])(
    "RowData.getNumber($columnLabel) -> value is $type -> should throw ColumnTypeError",
    async ({ rows, type, columnLabel }) => {
      connectionPool.query.mockResolvedValue({ rows });
      const errorMessage = `Expected column '${columnLabel}' to be a 'number', received '${type}'`;

      const statement = connection.createStatement("SELECT * FROM table");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getNumber(columnLabel);

      assert.throws(action, ColumnTypeError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ isAdmin: true }],
      columnLabel: "isAdmin",
      expectedValue: true,
    },
    {
      rows: [{ isOk: false }],
      columnLabel: "isOk",
      expectedValue: false,
    },
    {
      rows: [{ isActive: null }],
      columnLabel: "isActive",
      expectedValue: null,
    },
  ])(
    "RowData.getBoolean($columnLabel) -> value is a boolean or null -> should return $expectedValue",
    async ({ rows, columnLabel, expectedValue }) => {
      connectionPool.query.mockResolvedValue({ rows });

      const statement = connection.createStatement("SELECT * FROM tables");
      const rowData = await statement.execute();

      expect(rowData.at(0)?.getBoolean(columnLabel)).toBe(expectedValue);
    }
  );

  test.each([
    {
      columnLabel: "isAdmin",
    },
    {
      columnLabel: "isActive",
    },
    {
      columnLabel: "isDisabled",
    },
  ])(
    "RowData.getBoolean($columnLabel) -> value is undefined -> should throw a ColumnNotFoundError",
    async ({ columnLabel }) => {
      connectionPool.query.mockResolvedValue({ rows: [{}] });
      const errorMessage = `Column '${columnLabel}' not found`;

      const statement = connection.createStatement("SELECT * FROM tables");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getBoolean(columnLabel);

      assert.throws(action, ColumnNotFoundError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ name: "John" }],
      type: "string",
      columnLabel: "name",
    },
    {
      rows: [{ age: 69 }],
      type: "number",
      columnLabel: "age",
    },
    {
      rows: [{ dob: new Date("2004-12-13") }],
      type: "Date",
      columnLabel: "dob",
    },
    {
      rows: [{ obj: { name: "Dick" } }],
      type: "unknown",
      columnLabel: "obj",
    },
  ])(
    "RowData.getBoolean($columnLabel) -> value is not a boolean -> should throw ColumnTypeError",
    async ({ rows, columnLabel, type }) => {
      connectionPool.query.mockResolvedValue({ rows });
      const errorMessage = `Expected column '${columnLabel}' to be a 'boolean', received '${type}' instead`;

      const statement = connection.createStatement("SELECT * FROM tables");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getBoolean(columnLabel);

      assert.throws(action, ColumnTypeError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ dob: new Date("2004-12-13") }],
      columnLabel: "dob",
      expectedValue: new Date("2004-12-13"),
    },
    {
      rows: [{ registration_date: "2006-10-14" }],
      columnLabel: "registration_date",
      expectedValue: new Date("2006-10-14"),
    },
    {
      rows: [{ last_update: null }],
      columnLabel: "last_update",
      expectedValue: null,
    },
  ])(
    "RowData.getDate($columnLabel) -> value is valid date, date string or null -> should return $expectedValue",
    async ({ rows, columnLabel, expectedValue }) => {
      connectionPool.query.mockResolvedValue({ rows });

      const statement = connection.createStatement("SELECT * FROM tables");
      const rowData = await statement.execute();

      expect(rowData.at(0)?.getDate(columnLabel)).toEqual(expectedValue);
    }
  );

  test.each([
    {
      columnLabel: "dob",
    },
    {
      columnLabel: "lastUpdate",
    },
    {
      columnLabel: "delivery_date",
    },
  ])(
    "RowData.getDate($columnLabel) -> value is undefined -> should throw ColumnNotFoundError",
    async ({ columnLabel }) => {
      connectionPool.query.mockResolvedValue({ rows: [{}] });
      const errorMessage = `Column '${columnLabel}' was not found`;

      const statement = connection.createStatement("SELECT * FROM users");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getDate(columnLabel);

      assert.throws(action, ColumnNotFoundError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ age: 20 }],
      type: "number",
      columnLabel: "age",
    },
    {
      rows: [{ isAdmin: true }],
      type: "boolean",
      columnLabel: "isAdmin",
    },
    {
      rows: [{ name: "Aurora" }],
      type: "string",
      columnLabel: "name",
    },
    {
      rows: [{ invalid_date_string: "2003-18-300" }],
      type: "string",
      columnLabel: "invalid_date_string",
    },
    {
      rows: [{ obj: { name: "Shayne" } }],
      type: "unknown",
      columnLabel: "obj",
    },
  ])(
    "RowData.getDate($columnLabel) -> value is not a Date -> should throw ColumnTypeError",
    async ({ rows, columnLabel, type }) => {
      connectionPool.query.mockResolvedValue({ rows });
      const errorMessage = `Expected column '${columnLabel}' to be a 'Date', received '${type}' instead`;

      const statement = connection.createStatement("SELECT * FROM users");
      const rowData = await statement.execute();
      const action = () => rowData.at(0)?.getDate(columnLabel);

      assert.throws(action, ColumnTypeError, errorMessage);
    }
  );

  test.each([
    {
      rows: [{ name: "John" }, { name: "Smith" }, { name: "Doe" }],
      rowLength: 3,
      expectedLength: 3,
    },
    {
      rows: [{ age: 19 }, { age: 20 }, { age: 21 }, { age: 22 }, { age: 23 }, { age: 25 }],
      rowLength: 6,
      expectedLength: 6,
    },
  ])(
    "RowData -> rows length is $rowLength -> rowData length should be $expectedLength",
    async ({ rows }) => {
      connectionPool.query.mockResolvedValue({ rows });

      const statement = connection.createStatement("SELECT * FROM users");
      const rowData = await statement.execute();

      expect(rowData.length).toBe(rows.length);
    }
  );
});
