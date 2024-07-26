import { date, safeParse } from "valibot";

export class ColumnNotFoundError extends Error {}
export class ColumnTypeError extends TypeError {}

export abstract class RowData {
  constructor(private data: any) {}

  getString(columnLabel: string): string | null {
    const value = this.data[columnLabel];
    if (value === undefined) throw new ColumnNotFoundError(`Column '${columnLabel}' was not found`);
    if (value === null) return null;
    const expectedType = "string";
    if (RowData.isNumber(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "number"));
    if (RowData.isBoolean(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "boolean"));
    if (RowData.isDate(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "Date"));
    if (!RowData.isString(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "unknown"));
    return value as string;
  }

  getNumber(columnLabel: string): number | null {
    const value = this.data[columnLabel];
    if (value === undefined) throw new ColumnNotFoundError(`Column '${columnLabel}' was not found`);
    if (value === null) return null;
    const expectedType = "number";
    if (RowData.isString(value) && !RowData.isStringNumber(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "string"));
    if (RowData.isBoolean(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "boolean"));
    if (RowData.isDate(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "Date"));
    if (RowData.isStringNumber(value)) return Number(value);
    return value as number;
  }

  getBoolean(columnLabel: string): boolean | null {
    const value = this.data[columnLabel];
    if (value === undefined) throw new ColumnNotFoundError(`Column '${columnLabel}' not found`);
    if (value === null) return null;
    const expectedType = "boolean";
    if (RowData.isString(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "string"));
    if (RowData.isNumber(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "number"));
    if (RowData.isDate(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "Date"));
    if (!RowData.isBoolean(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "unknown"));
    return value as boolean;
  }

  getDate(columnLabel: string): Date | null {
    const value = this.data[columnLabel];
    if (value === undefined) throw new ColumnNotFoundError(`Column '${columnLabel}' was not found`);
    if (value === null) return null;
    const expectedType = "Date";
    if (RowData.isNumber(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "number"));
    if (RowData.isBoolean(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "boolean"));
    if (!RowData.isDate(value) && !RowData.isString(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "unknown"));
    if (!RowData.isValidDateString(value))
      throw new ColumnTypeError(RowData.formatError(columnLabel, expectedType, "string"));
    return RowData.isDate(value) ? value : new Date(value);
  }

  private static isBoolean(value: unknown): boolean {
    return typeof value === "boolean";
  }

  private static isNumber(value: unknown): boolean {
    return typeof value === "number";
  }

  private static isDate(value: unknown): boolean {
    return safeParse(date(), value).success;
  }

  private static isString(value: unknown): boolean {
    return typeof value === "string";
  }

  private static isValidDateString(value: string): boolean {
    const date = new Date(value);
    return !isNaN(date.valueOf());
  }

  private static isStringNumber(value: string): boolean {
    const number = Number(value);
    return !isNaN(number);
  }

  private static formatError(
    columnLabel: string,
    expectedType: string,
    actualType: string
  ): string {
    return `Expected column '${columnLabel}' to be a '${expectedType}', received '${actualType}' instead`;
  }
}

export interface Statement {
  execute(): Promise<RowData[]>;
}

export interface PreparedStatement extends Statement {
  setString(value: string): PreparedStatement;
  setNumber(value: number): PreparedStatement;
  setBoolean(value: boolean): PreparedStatement;
  setDate(value: Date): PreparedStatement;
}

export interface CanCreateStatement {
  createStatement(SQL: string): Statement;
  prepareStatement(SQL: string): PreparedStatement;
}

export interface DatabaseConnection extends CanCreateStatement {
  close(): Promise<void>;
}
