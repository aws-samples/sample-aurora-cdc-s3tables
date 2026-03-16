import * as cdk from 'aws-cdk-lib';
import * as s3tables from 'aws-cdk-lib/aws-s3tables';
import { Construct } from 'constructs';
import { CONFIG } from './config';

type SF = s3tables.CfnTable.SchemaFieldProperty;

const SCHEMAS: Record<string, SF[]> = {
  orders: [
    { name: 'order_id', type: 'int', required: true },
    { name: 'customer_id', type: 'int' },
    { name: 'order_date', type: 'string' },
    { name: 'total_amount', type: 'decimal(12,2)' },
    { name: 'status', type: 'string' },
    { name: 'created_at', type: 'timestamp' },
    { name: 'updated_at', type: 'timestamp' },
  ],
  products: [
    { name: 'product_id', type: 'int', required: true },
    { name: 'product_name', type: 'string' },
    { name: 'category', type: 'string' },
    { name: 'price', type: 'decimal(10,2)' },
    { name: 'stock_quantity', type: 'int' },
    { name: 'created_at', type: 'timestamp' },
    { name: 'updated_at', type: 'timestamp' },
  ],
};

export class S3TablesStack extends cdk.Stack {
  public readonly tableBucketArn: string;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const bucket = new s3tables.CfnTableBucket(this, 'TableBucket', {
      tableBucketName: CONFIG.s3TablesBucketName,
    });
    this.tableBucketArn = bucket.attrTableBucketArn;

    const ns = new s3tables.CfnNamespace(this, 'Namespace', {
      tableBucketArn: bucket.attrTableBucketArn,
      namespace: CONFIG.s3TablesNamespace,
    });
    ns.addDependency(bucket);

    for (const t of CONFIG.tables) {
      const table = new s3tables.CfnTable(this, `Table-${t}`, {
        tableBucketArn: bucket.attrTableBucketArn,
        namespace: CONFIG.s3TablesNamespace,
        tableName: t,
        openTableFormat: 'ICEBERG',
        icebergMetadata: { icebergSchema: { schemaFieldList: SCHEMAS[t] } },
      });
      table.addDependency(ns);
    }

    new cdk.CfnOutput(this, 'TableBucketArn', { value: bucket.attrTableBucketArn });
  }
}
