import { beforeEach, describe, expect, test } from "vitest";
import { mock, MockProxy } from "vitest-mock-extended";
import { PGConnectionPool, PostgresConnectionAdapter } from "../src/postgres";

describe("PreparedStatement", () => {
  let connectionPool: MockProxy<PGConnectionPool>;
  let connection: PostgresConnectionAdapter;

  beforeEach(() => {
    connectionPool = mock<PGConnectionPool>();
    connection = new PostgresConnectionAdapter(connectionPool);
  });

  test.each([
    {
      SQLString: "INSERT INTO users VALUES (?, ?, ?, ?)",
      expectedSQLCalled: "INSERT INTO users VALUES ($1, $2, $3, $4)",
      rows: [{ id: 1, name: "John", isAdmin: true, dob: new Date("2004-12-13") }],
      parameters: [1, "John", true, new Date("2004-12-13")],
    },
  ])(
    "PreparedStatement.execute -> given SQL string $SQLString' -> should call connectionPool with $expectedSQLCalled",
    async ({ SQLString, expectedSQLCalled, rows, parameters }) => {
      connectionPool.query.mockResolvedValue({ rows });

      await connection
        .prepareStatement(SQLString)
        .setNumber(parameters[0] as number)
        .setString(parameters[1] as string)
        .setBoolean(parameters[2] as boolean)
        .setDate(parameters[3] as Date)
        .execute();

      expect(connectionPool.query).toHaveBeenCalledOnce();
      expect(connectionPool.query).toHaveBeenCalledWith(expectedSQLCalled, parameters);
    }
  );
});
