import { Pool as PGConnectionPool } from "pg";
import { DatabaseConnection, PreparedStatement, RowData, Statement } from "./tsdbc";
export { PGConnectionPool };

class PostgresRowData extends RowData {}

class PostgresStatement implements Statement {
  constructor(private connectionPool: PGConnectionPool, private SQL: string) {}

  protected getConnectionPool(): PGConnectionPool {
    return this.connectionPool;
  }

  protected getSQL(): string {
    return this.SQL;
  }

  async execute(): Promise<RowData[]> {
    const results = await this.connectionPool.query(this.SQL);
    return results.rows.map((data) => new PostgresRowData(data));
  }
}

class PostgresPreparedStatement extends PostgresStatement implements PreparedStatement {
  private parameters: any[];

  constructor(connectionPool: PGConnectionPool, SQL: string) {
    super(connectionPool, PostgresPreparedStatement.transFrom(SQL));
    this.parameters = [];
  }

  private static transFrom(SQL: string): string {
    let properSQLString = "";
    let parameterIndex = 0;

    for (let index = 0; index < SQL.length; index++) {
      if (SQL[index] == "?") {
        parameterIndex++;
        properSQLString += `$${parameterIndex}`;
      } else properSQLString += SQL[index];
    }
    return properSQLString;
  }

  setString(value: string): PreparedStatement {
    this.parameters.push(value);
    return this;
  }

  setNumber(value: number): PreparedStatement {
    this.parameters.push(value);
    return this;
  }

  setBoolean(value: boolean): PreparedStatement {
    this.parameters.push(value);
    return this;
  }

  setDate(value: Date): PreparedStatement {
    this.parameters.push(value);
    return this;
  }

  async execute(): Promise<RowData[]> {
    const connectionPool = this.getConnectionPool();
    const SQL = this.getSQL();
    const results = await connectionPool.query(SQL, this.parameters);
    return results.rows.map((data) => new PostgresRowData(data));
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
    return new PostgresPreparedStatement(this.connectionPool, SQL);
  }
}
