import { RDSDataClient } from "@aws-sdk/client-rds-data";

export function sayHello() {
    console.log("hi");
}
export function sayGoodbye() {
    console.log("goodbye");
}

export class Crust {
    constructor(private readonly rdsClient: RDSDataClient) {}
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
