import {
    ColumnMetadata,
    ExecuteStatementCommand,
    Field,
    LongReturnType,
    RDSDataClient,
} from "@aws-sdk/client-rds-data";

export class Crust {
    constructor(
        private readonly rdsClient: RDSDataClient,
        private readonly resourceArn: string,
        private readonly secretArn: string,
        private readonly database: string
    ) {}

    async query(sql: string) {
        const { columnMetadata, records } = await this.rdsClient.send(
            new ExecuteStatementCommand({
                sql,
                resourceArn: this.resourceArn,
                secretArn: this.secretArn,
                database: this.database,
                includeResultMetadata: true,
                resultSetOptions: {
                    longReturnType: LongReturnType.STRING,
                },
            })
        );

        console.debug(columnMetadata, records);

        if (!columnMetadata) throw new Error("FIXME: no columnMetadata");

        if (!records) throw new Error("FIXME: no records");

        return parse(columnMetadata, records);
    }
}

export function parse(columns: ColumnMetadata[], rows: Field[][]) {
    const parser = _buildRowParser(columns);

    return rows?.map((r) =>
        r.reduce((a: Record<string, any>, f, i) => {
            a[parser[i].columnName] = parser[i].parser(f);
            return a;
        }, {})
    );
}

type ColumnParser = {
    columnName: string;
    parser: (field: Field) => any;
};

export function _buildRowParser(columns: ColumnMetadata[]): ColumnParser[] {
    return columns.map((c) => {
        if (!c.name) {
            throw new Error("FIXME: where's the column name?");
        } else if (!c.typeName) {
            throw new Error("FIXME: where's the column data type?");
        }

        const parser = dataTypeParsers.get(c.typeName);

        if (!parser) {
            throw new Error(`FIXME: where's ${c.typeName} parser?`);
        }

        return {
            columnName: c.name,
            parser,
        };
    });
}

enum MySqlDataTypeName {
    BIGINT = "BIGINT",
    BIGINT_UNSIGNED = "BIGINT UNSIGNED",
    BIT = "BIT",
    CHAR = "CHAR",
    DATE = "DATE",
    DATETIME = "DATETIME",
    DECIMAL = "DECIMAL",
    DECIMAL_UNSIGNED = "DECIMAL UNSIGNED",
    DOUBLE = "DOUBLE",
    DOUBLE_UNSIGNED = "DOUBLE UNSIGNED",
    FLOAT = "FLOAT",
    FLOAT_UNSIGNED = "FLOAT UNSIGNED",
    INT = "INT",
    INT_UNSIGNED = "INT UNSIGNED",
    JSON = "JSON",
    MEDIUMINT = "MEDIUMINT",
    MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED",
    SMALLINT = "SMALLINT",
    SMALLINT_UNSIGNED = "SMALLINT UNSIGNED",
    TEXT = "TEXT",
    TIME = "TIME",
    TIMESTAMP = "TIMESTAMP",
    TINYINT = "TINYINT",
    TINYINT_UNSIGNED = "TINYINT UNSIGNED",
    VARCHAR = "VARCHAR",
    YEAR = "YEAR",
}

const dataTypeParsers = new Map<string, (field: Field) => any>([
    [MySqlDataTypeName.BIGINT_UNSIGNED, _parseStringField],
    [MySqlDataTypeName.BIGINT, _parseBigIntegerField],
    [MySqlDataTypeName.BIT, _parseBooleanField],
    [MySqlDataTypeName.CHAR, _parseStringField],
    [MySqlDataTypeName.DATE, _parseStringField],
    [MySqlDataTypeName.DATETIME, _parseStringField],
    [MySqlDataTypeName.DECIMAL_UNSIGNED, _parseFloatField],
    [MySqlDataTypeName.DECIMAL, _parseFloatField],
    [MySqlDataTypeName.DOUBLE_UNSIGNED, _parseFloatField],
    [MySqlDataTypeName.DOUBLE, _parseFloatField],
    [MySqlDataTypeName.FLOAT_UNSIGNED, _parseFloatField],
    [MySqlDataTypeName.FLOAT, _parseFloatField],
    [MySqlDataTypeName.INT_UNSIGNED, _parseIntegerField],
    [MySqlDataTypeName.INT, _parseIntegerField],
    [MySqlDataTypeName.JSON, _parseJsonField],
    [MySqlDataTypeName.MEDIUMINT_UNSIGNED, _parseIntegerField],
    [MySqlDataTypeName.MEDIUMINT, _parseIntegerField],
    [MySqlDataTypeName.SMALLINT_UNSIGNED, _parseIntegerField],
    [MySqlDataTypeName.SMALLINT, _parseIntegerField],
    [MySqlDataTypeName.TEXT, _parseStringField],
    [MySqlDataTypeName.TIME, _parseStringField],
    [MySqlDataTypeName.TIMESTAMP, _parseStringField],
    [MySqlDataTypeName.TINYINT_UNSIGNED, _parseIntegerField],
    [MySqlDataTypeName.TINYINT, _parseIntegerField],
    [MySqlDataTypeName.VARCHAR, _parseStringField],
    [MySqlDataTypeName.YEAR, _parseStringField],
]);

// FIXME: null value handling

export function _parseBooleanField(field: Field) {
    if (!field.booleanValue) throw new Error("FIXME: wrong field");

    return field.booleanValue;
}

export function _parseFloatField(field: Field) {
    if (!field.doubleValue) throw new Error("FIXME: wrong field");

    return field.doubleValue;
}

// NOTE: Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER
export function _parseIntegerField(field: Field) {
    if (!field.stringValue) throw new Error("FIXME: wrong field");

    return Number.parseInt(field.stringValue);
}

export function _parseStringField(field: Field) {
    if (!field.stringValue) throw new Error("FIXME: wrong field");

    return field.stringValue;
}

export function _parseBigIntegerField(field: Field) {
    if (!field.stringValue) throw new Error("FIXME: wrong field");

    return BigInt(field.stringValue);
}

export function _parseJsonField(field: Field) {
    if (!field.stringValue) throw new Error("FIXME: wrong field");

    return JSON.parse(field.stringValue);
}

// type FieldValueType =
//     | "arrayValue"
//     | "blobValue"
//     | "booleanValue"
//     | "doubleValue"
//     | "isNull"
//     | "longValue"
//     | "stringValue";

// FIXME: clean up

// type ColumnMetadata = {
//     arrayBaseColumnType: number;
//     isAutoIncrement: boolean;
//     isCaseSensitive: false;
//     isCurrency: false;
//     isSigned: false;
//     label: string;
//     name: string;
//     nullable: number;
//     precision: number;
//     scale: number;
//     schemaName: string;
//     tableName: string;
//     type: number;
//     typeName: ColumnTypeName;
// };

// type RecordType = "isNull" | "stringValue" | "longValue";
