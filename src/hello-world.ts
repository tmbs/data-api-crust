import {
    ExecuteStatementCommand,
    RDSDataClient,
} from "@aws-sdk/client-rds-data";

export function sayHello() {
    console.log("hi");
}
export function sayGoodbye() {
    console.log("goodbye");
}

export class Crust {
    constructor(
        private readonly rdsClient: RDSDataClient,
        private readonly resourceArn: string,
        private readonly secretArn: string,
        private readonly database: string
    ) {}

    async query(sql: string) {
        const { records, columnMetadata } = await this.rdsClient.send(
            new ExecuteStatementCommand({
                sql,
                resourceArn: this.resourceArn,
                secretArn: this.secretArn,
                database: this.database,
                includeResultMetadata: true,
            })
        );

        return records?.map((r) => {
            const record: { [key: string]: any } = {};

            r.map((v, i) => {
                const name = columnMetadata?.[i].name;

                if (name) {
                    record[name!] = v;
                }
            });

            return record;
        });
    }
}

type ColumnTypeName =
    | "BIGINT UNSIGNED"
    | "BIT"
    | "CHAR"
    | "INT UNSIGNED"
    | "JSON"
    | "TIMESTAMP";

type ColumnMetadata = {
    arrayBaseColumnType: number;
    isAutoIncrement: boolean;
    isCaseSensitive: false;
    isCurrency: false;
    isSigned: false;
    label: string;
    name: string;
    nullable: number;
    precision: number;
    scale: number;
    schemaName: string;
    tableName: string;
    type: number;
    typeName: ColumnTypeName;
};

type RecordType = "isNull" | "stringValue" | "longValue";
