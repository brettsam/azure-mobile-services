// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

#import "MSQueuePullOperation.h"
#import "MSTableOperationError.h"
#import "MSSyncContextInternal.h"
#import "MSClientInternal.h"
#import "MSTableOperationInternal.h"
#import "MSQuery.h"
#import "MSSyncContext.h"
#import "MSSyncContextInternal.h"
#import "MSClientInternal.h"
#import "MSQueryInternal.h"
#import "MSNaiveISODateFormatter.h"
#import "MSDateOffset.h"

@interface MSQueuePullOperation()

@property (nonatomic, strong)   NSError *error;
@property (nonatomic, weak)     dispatch_queue_t dispatchQueue;
@property (nonatomic, weak)     NSOperationQueue *callbackQueue;
@property (nonatomic, weak)     MSSyncContext *syncContext;
@property (nonatomic, copy)     MSSyncBlock completion;
@property (nonatomic, strong)   MSQuery* query;
@property (nonatomic, strong)   NSString *queryId;
@property (nonatomic)           NSInteger recordsProcessed;
@property (nonatomic, strong)   NSDate *maxDate;
@property (nonatomic, strong)   NSDate *deltaToken;
@property (nonatomic, strong)   NSPredicate *originalPredicate;

@end

@implementation MSQueuePullOperation

- (id) initWithSyncContext:(MSSyncContext *)syncContext
                     query:(MSQuery *)query
                   queryId:(NSString *)queryId
             dispatchQueue:(dispatch_queue_t)dispatchQueue
             callbackQueue:(NSOperationQueue *)callbackQueue
                completion:(MSSyncBlock)completion
{
    self = [super init];
    if (self) {
        _syncContext = syncContext;
        _query = query;
        _queryId = queryId;
        _dispatchQueue = dispatchQueue;
        _callbackQueue = callbackQueue;
        _completion = [completion copy];
        _recordsProcessed = 0;
        _maxDate = [NSDate distantPast];
        _deltaToken = nil;
        _originalPredicate = self.query.predicate;
    }
    return self;
}

- (void) completeOperation {
    [self willChangeValueForKey:@"isFinished"];
    [self willChangeValueForKey:@"isExecuting"];
        
    executing_ = NO;
    finished_ = YES;
    
    [self didChangeValueForKey:@"isExecuting"];
    [self didChangeValueForKey:@"isFinished"];
}

// Check if the operation was cancelled and if so, begin winding down
-(BOOL) checkIsCanceled
{
    if (self.isCancelled) {
        self.error = [self errorWithDescription:@"Push cancelled" code:MSPushAbortedUnknown internalError:nil];
        [self completeOperation];
    }
    
    return self.isCancelled;
}

-(void) start
{
    if (finished_) {
        return;
    }
    else if (self.isCancelled) {
        [self completeOperation];
        return;
    }
    
    [self willChangeValueForKey:@"isExecuting"];
    executing_ = YES;
    [self didChangeValueForKey:@"isExecuting"];
    
    if (self.queryId) {
        self.query.table.systemProperties |= MSSystemPropertyUpdatedAt;
        dispatch_sync(self.dispatchQueue, ^{
            NSError *localDataSourceError;
            [self updateQueryFromDeltaTokenOrError:&localDataSourceError];
            if([self callCompletionIfError:localDataSourceError])
            {
                return;
            }
        });
    }
    
    [self processPullOperation];
}

/// For a given pending table operation, create the request to send it to the remote table
- (void) processPullOperation
{
    // Read from server
    [self.query readInternalWithFeatures:MSFeatureOffline completion:^(MSQueryResult *result, NSError *error) {
        // If error, or no results we can stop processing
        if (error || !result || !result.items || result.items.count == 0) {
            if (self.completion) {
                [self.callbackQueue addOperationWithBlock:^{
                    self.completion(error);
                }];
            }
            [self completeOperation];
            return;
        }
        
        // Update our local store (we need to block inbound operations while we do this)
        dispatch_sync(self.dispatchQueue, ^{
            NSError *localDataSourceError;
            
            // Check if have any pending ops on this table
            NSArray *pendingOps = [self.syncContext.operationQueue getOperationsForTable:self.query.table.name item:nil];
            
            NSMutableArray *itemsToUpsert = [NSMutableArray array];
            NSMutableArray *itemIdsToDelete = [NSMutableArray array];
            
            [result.items enumerateObjectsUsingBlock:^(id obj, NSUInteger idx, BOOL *stop) {
                if (self.queryId) {
                    self.maxDate = [self.maxDate laterDate:(NSDate *)obj[MSSystemColumnUpdatedAt]];
                }
                BOOL isDeleted =  ((NSNumber *)obj[MSSystemColumnDeleted]).boolValue;
                
                NSPredicate *predicate = [NSPredicate predicateWithFormat:@"%K == %@", @"itemId", obj[MSSystemColumnId]];
                NSArray *matchingRecords = [pendingOps filteredArrayUsingPredicate:predicate];
                
                // we want to ignore items that have been touched since the Pull was started
                if (matchingRecords.count == 0) {
                    if (isDeleted == YES) {
                        [itemIdsToDelete addObject:obj[MSSystemColumnId]];
                    }
                    else {
                        [itemsToUpsert addObject:obj];
                    }
                }
            }];
            
            [self.syncContext.dataSource deleteItemsWithIds:itemIdsToDelete table:self.query.table.name orError:&localDataSourceError];
            if ([self callCompletionIfError:localDataSourceError]) {
                return;
            }
            
            // upsert each item into table that isn't pending to go to the server
            [self.syncContext.dataSource upsertItems:itemsToUpsert table:self.query.table.name orError:&localDataSourceError];
            if ([self callCompletionIfError:localDataSourceError]) {
                return;
            }
            
            self.recordsProcessed += result.items.count;
            
            if (self.queryId) {
                if (!self.deltaToken || [self.deltaToken compare:self.maxDate] == NSOrderedAscending) {
                    // if we have no deltaToken or the maxDate has increased, store it, and requery
                    [self upsertDeltaTokenOrError:&localDataSourceError];
                    if([self callCompletionIfError:localDataSourceError]) {
                        return;
                    }
                    
                    self.recordsProcessed = 0;
                    
                    [self updateQueryFromDeltaTokenOrError:&localDataSourceError];
                    if ([self callCompletionIfError:localDataSourceError]) {
                        return;
                    }
                }
                else {
                    self.query.fetchOffset = self.recordsProcessed;
                }
            }
            else {
                self.query.fetchOffset = self.recordsProcessed;
            }
            
            // try to Pull again with the updated offset or query
            [self processPullOperation];
        });
    }];
}

-(void) upsertDeltaTokenOrError:(NSError **)error
{
    NSDateFormatter *formatter = [MSNaiveISODateFormatter naiveISODateFormatter];
    NSString *configQueryId = [self deltaTokenKey];
    NSDictionary *delta = @{@"id":configQueryId, @"value":[formatter stringFromDate:self.maxDate]};
    [self.syncContext.dataSource upsertItems:@[delta] table:self.syncContext.dataSource.configTableName orError:error];
    if (error && *error) {
        return;
    }
    self.deltaToken = self.maxDate;
}

-(void) updateQueryFromDeltaTokenOrError:(NSError **)error
{
    // only load from local database if nil; we update it when writing
    if (!self.deltaToken) {
        NSDateFormatter *formatter = [MSNaiveISODateFormatter naiveISODateFormatter];
        NSString *configQueryId = [self deltaTokenKey];
        NSDictionary *deltaTokenDict = [self.syncContext.dataSource readTable:self.syncContext.dataSource.configTableName withItemId:configQueryId orError:error];
        if (error && *error) {
            return;
        }
        self.deltaToken = [formatter dateFromString:deltaTokenDict[@"value"]];
    }
    
    self.query.fetchOffset = -1;
    
    if (self.deltaToken) {
        MSDateOffset *offset = [[MSDateOffset alloc]initWithDate:self.deltaToken];
        NSPredicate *updatedAt = [NSPredicate predicateWithFormat:@"%K >= %@", MSSystemColumnUpdatedAt, offset];
        if (self.originalPredicate) {
            self.query.predicate = [NSCompoundPredicate andPredicateWithSubpredicates:@[self.originalPredicate, updatedAt]];
        }
        else {
            self.query.predicate = updatedAt;
        }
    }
}

-(NSString *)deltaTokenKey
{
    return [NSString stringWithFormat:@"deltaToken|%@|%@", self.query.table.name, self.queryId];
}

-(BOOL) callCompletionIfError:(NSError *)error
{
    BOOL isError = NO;
    if (error) {
        isError = YES;
        if (self.completion) {
            [self.callbackQueue addOperationWithBlock:^{
                self.completion(error);
            }];
        }
        [self completeOperation];
    }
    return isError;
}

/// Builds a NSError containing the errors related to a push operation
- (NSError *) errorWithDescription:(NSString *)description code:(NSInteger)code internalError:(NSError *)error
{
    NSMutableDictionary *userInfo = [@{ NSLocalizedDescriptionKey: description } mutableCopy];
    
    if (error) {
        [userInfo setObject:error forKey:NSUnderlyingErrorKey];
    }
    
    return [NSError errorWithDomain:MSErrorDomain code:code userInfo:userInfo];
}

/// Builds a NSError containing the errors related to a push operation
-(NSError *) errorWithDescription:(NSString *)description code:(NSInteger)code pushErrors:(NSArray *)pushErrors internalError:(NSError *)error
{
    NSMutableDictionary *userInfo = [@{ NSLocalizedDescriptionKey: description } mutableCopy];
    
    if (error) {
        [userInfo setObject:error forKey:NSUnderlyingErrorKey];
    }
    
    if (pushErrors && pushErrors.count > 0) {
        [userInfo setObject:pushErrors forKey:MSErrorPushResultKey];
    }
    
    return [NSError errorWithDomain:MSErrorDomain code:code userInfo:userInfo];
}

- (BOOL) isConcurrent {
    return YES;
}

- (BOOL) isExecuting {
    return executing_;
}

- (BOOL) isFinished {
    return finished_;
}

@end
