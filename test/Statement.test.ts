import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { mock, mockClear, MockProxy } from "vitest-mock-extended";
import { PGConnectionPool, PostgresConnection } from "../src/postgres";

describe("Statement", () => {
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
      rows: [{ name: "John Smith" }],
      SQL: "SELECT * FROM users",
    },
    {
      rows: [{ age: 20 }, { age: 19 }],
      SQL: "SELECT age FROM users",
    },
  ])(
    "Statement -> given SQL string $SQL -> should call connectionPool.query with $SQL",
    async ({ rows, SQL }) => {
      connectionPool.query.mockResolvedValue({ rows });

      const statement = connection.createStatement(SQL);
      await statement.execute();

      expect(connectionPool.query).toHaveBeenCalledWith(SQL);
    }
  );
});
