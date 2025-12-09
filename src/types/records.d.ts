export interface RowStatus {
  csvRecordId: string;
  first_name: string;
  last_name: string;
  mailing_address: string;
  property_address: string[];
  lists: string[];
  jobId: number;
  contactId?: string;
  directSkipId?: number;
  skipTracedAt?: Date | null;
  directSkipStatus: DirectSkipStatus | null;
  userDecision: RowDecisionStatus;
  decidedAt?: Date | null;
  // Include all other fields from MongoDB as flexible data
  [key: string]: any;
}

export interface JobCheckResult {
  jobId: number;
  startedByBot: string;
  flow: string[];
  records: RowStatus[];
}