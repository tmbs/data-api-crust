import {
    ColumnMetadata,
    ExecuteStatementCommand,
    Field,
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
            })
        );

        if (!columnMetadata) throw new Error("FIXME: no columnMetadata");

        if (!records) throw new Error("FIXME: no records");

        return parse(columnMetadata, records);
    }
}

export function parse(columns: ColumnMetadata[], rows: Field[][]) {
    return rows?.map((r) => {
        const record: { [key: string]: any } = {};

        r.map((v, i) => {
            const name = columns?.[i].name;

            if (name) {
                record[name!] = v;
            }
        });

        return record;
    });
}

// FIXME: clean up

// type ColumnTypeName =
//     | "BIGINT UNSIGNED"
//     | "BIT"
//     | "CHAR"
//     | "INT UNSIGNED"
//     | "JSON"
//     | "TIMESTAMP";

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
