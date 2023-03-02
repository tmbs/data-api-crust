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
    return rows?.map((r) =>
        r.reduce((a: Record<string, any>, f, i) => {
            a[columns[i].name!] = f.arrayValue
                ? f.arrayValue
                : f.blobValue
                ? f.blobValue
                : f.booleanValue
                ? f.booleanValue
                : f.doubleValue
                ? f.doubleValue
                : f.isNull
                ? f.isNull
                : f.longValue
                ? f.longValue
                : f.stringValue
                ? f.stringValue
                : "FIXME:";

            return a;
        }, {})
    );
}

type Parser = ((field: Field) => any)[];

export function _buildParser(columns: ColumnMetadata[]): Parser {
    return columns.map((c) => {
        switch (c.typeName) {
            case MySqlDataTypeName.BIGINT_UNSIGNED:
                return _parseIntegerField;
            case MySqlDataTypeName.BIT:
                return _parseBooleanField;
            default:
                throw new Error("FIXME: implemented");
        }
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
    TINYINT_UNSIGNED = "TINYINT UNSIGNE",
    VARCHAR = "VARCHAR",
    YEAR = "YEAR",
}

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
