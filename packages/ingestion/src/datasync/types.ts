export type DataSyncError = {
  error: string;
  message: string;
  code?: string;
  hint?: string;
};

export type DataSyncEvent = {
  id: string;
  sessionId?: string;
  userId?: string;
  type?: string;
  name?: string;
  timestamp: number | string;
  properties?: unknown;
  session?: unknown;
  [k: string]: unknown;
};

export type Pagination = {
  limit: number;
  hasMore: boolean;
  nextCursor?: string;
  cursorExpiresIn?: number;
};

export type FeedResponse = {
  data: DataSyncEvent[];
  pagination: Pagination;
  meta?: {
    total?: number;
    returned?: number;
    requestId?: string;
    [k: string]: unknown;
  };
};
