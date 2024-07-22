import { Pool as PGConnectionPool } from "pg";
import { DatabaseConnection, PreparedStatement, RowData, Statement } from "./tsdbc";
export { PGConnectionPool };

class PostgresRowData extends RowData {}

class PostgresStatement implements Statement {
  constructor(private connectionPool: PGConnectionPool, private SQL: string) {}

  async execute(): Promise<RowData[]> {
    const results = await this.connectionPool.query(this.SQL);
    return results.rows.map((data) => new PostgresRowData(data));
  }
}

class PostgresPreparedStatement implements PreparedStatement {
  setString(value: string): PreparedStatement {
    throw new Error("Method not implemented.");
  }

  setNumber(value: string): PreparedStatement {
    throw new Error("Method not implemented.");
  }

  setBoolean(value: boolean): PreparedStatement {
    throw new Error("Method not implemented.");
  }

  setDate(value: Date): PreparedStatement {
    throw new Error("Method not implemented.");
  }

  execute(): Promise<RowData[]> {
    throw new Error("Method not implemented.");
  }
}

export class PostgresConnection implements DatabaseConnection {
  constructor(private connectionPool: PGConnectionPool) {}

  async close(): Promise<void> {
    await this.connectionPool.end();
  }

  createStatement(SQL: string): Statement {
    return new PostgresStatement(this.connectionPool, SQL);
  }

  prepareStatement(SQL: string): PreparedStatement {
    throw new Error("Method not implemented.");
  }
}
