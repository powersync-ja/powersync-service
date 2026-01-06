import { BucketPriority } from '../BucketDescription.js';

export interface StreamOptions {
  name: string;
  isSubscribedByDefault: boolean;
  priority: BucketPriority;
}
