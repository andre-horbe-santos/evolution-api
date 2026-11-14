import {
  type BucketLocationConstraint,
  CreateBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  ListBucketsCommand,
  PutBucketPolicyCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { ConfigService, S3 } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { BadRequestException } from '@exceptions';
import { join } from 'path';
import { Readable, Transform } from 'stream';

const logger = new Logger('S3 Service');

const BUCKET = new ConfigService().get<S3>('S3');
const DEFAULT_PRESIGNED_URL_EXPIRY_SECONDS = 7 * 24 * 60 * 60;

interface Metadata extends Record<string, string> {
  'Content-Type': string;
}

const s3Client = (() => {
  if (!BUCKET?.ENABLE) {
    return;
  }

  const endpoint = BUCKET.ENDPOINT.match(/^https?:\/\//)
    ? new URL(BUCKET.ENDPOINT)
    : new URL(`${BUCKET.USE_SSL ? 'https' : 'http'}://${BUCKET.ENDPOINT}`);

  if (!endpoint.port && BUCKET.PORT) {
    endpoint.port = String(BUCKET.PORT);
  }

  return new S3Client({
    endpoint: endpoint.toString(),
    forcePathStyle: true,
    region: BUCKET.REGION || 'us-east-1',
    credentials: {
      accessKeyId: BUCKET.ACCESS_KEY,
      secretAccessKey: BUCKET.SECRET_KEY,
    },
  });
})();

const bucketName = BUCKET.BUCKET_NAME;

const bucketExists = async () => {
  if (s3Client) {
    try {
      const list = await s3Client.send(new ListBucketsCommand({}));
      return list.Buckets?.find((bucket) => bucket.Name === bucketName);
    } catch {
      return false;
    }
  }
};

const setBucketPolicy = async () => {
  if (s3Client) {
    const policy = {
      Version: '2012-10-17',
      Statement: [
        {
          Effect: 'Allow',
          Principal: '*',
          Action: ['s3:GetObject'],
          Resource: [`arn:aws:s3:::${bucketName}/*`],
        },
      ],
    };

    await s3Client.send(
      new PutBucketPolicyCommand({
        Bucket: bucketName,
        Policy: JSON.stringify(policy),
      }),
    );
  }
};

const createBucket = async () => {
  if (s3Client) {
    try {
      const exists = await bucketExists();
      if (!exists) {
        const region = BUCKET.REGION || 'us-east-1';
        await s3Client.send(
          new CreateBucketCommand({
            Bucket: bucketName,
            ...(region === 'us-east-1'
              ? {}
              : {
                  CreateBucketConfiguration: {
                    LocationConstraint: region as BucketLocationConstraint,
                  },
                }),
          }),
        );
      }

      if (!BUCKET.SKIP_POLICY) {
        logger.warn('S3_SKIP_POLICY=false enables public read access to every object in the configured bucket.');
        await setBucketPolicy();
      }

      logger.info(`S3 Bucket ${bucketName} - ON`);
      return true;
    } catch (error) {
      logger.error('S3 ERROR:');
      logger.error(error);
      return false;
    }
  }
};

void createBucket();

const upload = async (objectName: string, file: Buffer | Transform | Readable, size: number, metadata: Metadata) => {
  if (s3Client) {
    try {
      return await s3Client.send(
        new PutObjectCommand({
          Bucket: bucketName,
          Key: objectName,
          Body: file,
          ContentLength: size,
          ContentType: metadata['Content-Type'],
          CacheControl: metadata['Cache-Control'],
          Metadata: { 'custom-header-application': 'evolution-api' },
        }),
      );
    } catch (error) {
      logger.error(error);
      return error;
    }
  }
};

const uploadFile = async (fileName: string, file: Buffer | Transform | Readable, size: number, metadata: Metadata) => {
  return upload(join('evolution-api', fileName), file, size, metadata);
};

const getObjectUrl = async (fileName: string, expiry?: number) => {
  if (s3Client) {
    try {
      const command = new GetObjectCommand({
        Bucket: bucketName,
        Key: join('evolution-api', fileName),
      });
      return await getSignedUrl(s3Client, command, {
        expiresIn: expiry || DEFAULT_PRESIGNED_URL_EXPIRY_SECONDS,
      });
    } catch (error) {
      throw new BadRequestException(error?.message);
    }
  }
};

const uploadTempFile = async (
  folder: string,
  fileName: string,
  file: Buffer | Transform | Readable,
  size: number,
  metadata: Metadata,
) => {
  return upload(join(folder, fileName), file, size, metadata);
};

const deleteFile = async (folder: string, fileName: string) => {
  if (s3Client) {
    try {
      return await s3Client.send(
        new DeleteObjectCommand({
          Bucket: bucketName,
          Key: join(folder, fileName),
        }),
      );
    } catch (error) {
      logger.error(error);
      return error;
    }
  }
};

export { BUCKET, deleteFile, getObjectUrl, uploadFile, uploadTempFile };
