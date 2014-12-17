// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

#import "MSSyncContext.h"
#import "MSSyncContextInternal.h"
#import "MSClientInternal.h"
#import "MSTable.h"
#import "MSTableOperationInternal.h"
#import "MSJSONSerializer.h"
#import "MSQuery.h"
#import "MSQueryInternal.h"
#import "MSQueuePushOperation.h"
#import "MSQueuePullOperation.h"
#import "MSNaiveISODateFormatter.h"
#import "MSDateOffset.h"
#import "MSTableConfigValue.h"

@implementation MSSyncContext {
    dispatch_queue_t writeOperationQueue;
}

static NSOperationQueue *pushQueue_;

@synthesize delegate = delegate_;
@synthesize dataSource = dataSource_;
@synthesize operationQueue = operationQueue_;
@synthesize client = client_;
@synthesize callbackQueue = callbackQueue_;

-(void) setClient:(MSClient *)client
{
    client_ = client;
    operationQueue_ = [[MSOperationQueue alloc] initWithClient:client_ dataSource:self.dataSource];
    
    // We don't need to wait for this, and all operation creation goes onto this queue so its guaranteed to
    // happen only after this is populated.
    dispatch_async(writeOperationQueue, ^{
        self.operationSequence = [self.operationQueue getNextOperationId];
    });
}

-(id) init
{
    return [self initWithDelegate:nil dataSource:nil callback:nil];
}

-(id) initWithDelegate:(id<MSSyncContextDelegate>) delegate dataSource:(id<MSSyncContextDataSource>) dataSource callback:(NSOperationQueue *)callbackQueue
{
    self = [super init];
    if (self)
    {
        writeOperationQueue = dispatch_queue_create("WriteOperationQueue", DISPATCH_QUEUE_CONCURRENT);
        
        callbackQueue_ = callbackQueue;
        if (!callbackQueue_) {
            callbackQueue_ = [[NSOperationQueue alloc] init];
            callbackQueue_.name = @"Sync Context: Operation Callbacks";
            callbackQueue_.maxConcurrentOperationCount = 4;
        }
        
        pushQueue_ = [NSOperationQueue new];
        pushQueue_.maxConcurrentOperationCount = 1;
        pushQueue_.name = @"Sync Context: Push";
        
        dataSource_ = dataSource;
        delegate_ = delegate;
    }
    
    return self;
}

/// Return the number of pending operations (including in progress)
-(NSUInteger) pendingOperationsCount
{
    return [self.operationQueue count];
}

/// Begin sending pending operations to the remote tables. Abort the push attempt whenever any single operation
/// recieves an error due to network or authorization. Otherwise operations will all run and all errors returned
/// to the caller at once.
-(void) pushWithCompletion:(MSSyncBlock)completion
{
    // TODO: Allow users to cancel operations
    MSQueuePushOperation *push = [[MSQueuePushOperation alloc] initWithSyncContext:self
                                                                     dispatchQueue:writeOperationQueue
                                                                        completion:completion];
    
    [pushQueue_ addOperation:push];
}


#pragma mark private interface implementation


/// Given an item and an action to perform (insert, update, delete) determines how that should be represented
/// when sent to the server based on pending operations.
-(void) syncTable:(NSString *)table
             item:(NSDictionary *)item
           action:(MSTableOperationTypes)action
       completion:(MSSyncItemBlock)completion
{
    NSError *error;
    NSMutableDictionary *itemToSave = [item mutableCopy];
    NSString *itemId;
    
    // Validate our input and state
    if (!self.dataSource) {
        error = [self errorWithDescription:@"Missing required datasource for MSSyncContext"
                              andErrorCode:MSSyncContextInvalid];
    }
    else {
        // All sync table operations require a valid string Id
        itemId = [self.client.serializer stringIdFromItem:item orError:&error];
        if (error) {
            if (error.code == MSMissingItemIdWithRequest && action == MSTableOperationInsert) {
                itemId = [MSJSONSerializer generateGUID];
                [itemToSave setValue:itemId forKey:@"id"];
                error = nil;
            }
        }
    }
    
    if (error) {
        if (completion) {
            completion(nil, error);
        }
        return;
    }
    
    // Add the operation to the queue
    dispatch_async(writeOperationQueue, ^{
        NSError *error;
        MSCondenseAction condenseAction = MSCondenseAddNew;
        
        // Check if this table-item pair already has a pending operation and if so, how the new action
        // should be combined with the previous one
        NSArray *pendingActions = [self.operationQueue getOperationsForTable:table item:itemId];
        MSTableOperation *operation = [pendingActions lastObject];
        if (operation) {
            condenseAction = [MSTableOperation condenseAction:action withExistingOperation:operation];
            if (condenseAction == MSCondenseNotSupported) {
                error = [self errorWithDescription:@"The requested operation is not allowed due to an already pending operation"
                                      andErrorCode:MSSyncTableInvalidAction];
            }
        }
        
        if (condenseAction == MSCondenseAddNew) {
            operation = [MSTableOperation pushOperationForTable:table type:action itemId:itemId];
            operation.operationId = self.operationSequence;
            self.operationSequence++;
        }
        
        // Update local store and then the operation queue
        if (error == nil) {
            switch (action) {
                case MSTableOperationInsert:
                    [self.dataSource upsertItems:[NSArray arrayWithObject:itemToSave] table:table orError:&error];
                    break;
                    
                case MSTableOperationUpdate:
                    [self.dataSource upsertItems:[NSArray arrayWithObject:itemToSave] table:table orError:&error];
                    break;
                    
                case MSTableOperationDelete:
                    [self.dataSource deleteItemsWithIds:[NSArray arrayWithObject:itemId] table:table orError:&error];
                    
                    // Capture the deleted item in case the user wants to cancel it or a conflict occur
                    operation.item = item;
                    break;
                    
                default:
                    error = [self errorWithDescription:@"Unknown table action" andErrorCode:MSSyncTableInvalidAction];
                    break;
            }
        }
        
        if (error) {
            if (completion) {
                [self.callbackQueue addOperationWithBlock:^{
                    completion(nil, error);
                }];
            }
            return;
        }
        
        // Update the operation queue now
        if (condenseAction == MSCondenseAddNew) {
            [self.operationQueue addOperation:operation orError:&error];
        }
        else if (condenseAction == MSCondenseToDelete) {
            operation.type = MSTableOperationDelete;
            
            // FUTURE: Look at moving these upserts into the operation queue object
            [self.dataSource upsertItems:[NSArray arrayWithObject:[operation serialize]]
                                   table:[self.dataSource operationTableName]
                                 orError:&error];
            
        } else if (condenseAction != MSCondenseKeep) {
            [self.operationQueue removeOperation:operation orError:&error];
        }
        
        if (completion) {
            [self.callbackQueue addOperationWithBlock:^{
                completion(itemToSave, nil);
            }];
        }
    });
}

/// Simple passthrough to the local storage data source to retrive a single item using its Id
- (NSDictionary *) syncTable:(NSString *)table readWithId:(NSString *)itemId orError:(NSError **)error;
{
    return [self.dataSource readTable:table withItemId:itemId orError:error];
}

/// Assumes running with access to the operation queue
- (NSError *) removeOperation:(MSTableOperation *)operation
{
    NSError *error;
    [self.operationQueue removeOperation:operation orError:&error];
    return error;
}

/// Simple passthrough to the local storage data source to retrive a list of items
-(void)readWithQuery:(MSQuery *)query completion:(MSReadQueryBlock)completion {
    NSError *error;
    MSSyncContextReadResult *result = [self.dataSource readWithQuery:query orError:&error];
    
    if (completion) {
        if (error) {
            completion(nil, error);
        } else {
            MSQueryResult *queryResult = [[MSQueryResult alloc] initWithItems:result.items totalCount:result.totalCount nextLink:nil];
            completion(queryResult, nil);
        }
    }
}

/// Given a pending operation in the queue, removes it from the queue and updates the local store
/// with the given item
- (void) cancelOperation:(MSTableOperation *)operation updateItem:(NSDictionary *)item completion:(MSSyncBlock)completion;
{
    // Removing an operation requires write access to the queue
    dispatch_async(writeOperationQueue, ^{
        NSError *error;
        
        // FUTURE: Verify operation hasn't been modified by others
        
        // Remove system properties but keep __version
        NSMutableDictionary *itemToSave = [item mutableCopy];
        
        NSString *version = [itemToSave objectForKey:MSSystemColumnVersion];
        [self.client.serializer removeSystemProperties:itemToSave];
        if (version != nil) {
            [itemToSave setValue:version forKey:MSSystemColumnVersion];
        }
        
        [self.dataSource upsertItems:[NSArray arrayWithObject:itemToSave] table:operation.tableName orError:&error];
        if (!error) {
            [self.operationQueue removeOperation:operation orError:&error];
        }
        
        if (completion) {
            completion(error);
        }
    });
}

/// Given a pending operation in the queue, removes it from the queue and removes the item from the local
/// store.
- (void) cancelOperation:(MSTableOperation *)operation discardItemWithCompletion:(MSSyncBlock)completion
{
    // Removing an operation requires write access to the queue
    dispatch_async(writeOperationQueue, ^{
        NSError *error;
        
        // FUTURE: Verify operation hasn't been modified by others
        
        [self.dataSource deleteItemsWithIds:[NSArray arrayWithObject:operation.itemId]
                                      table:operation.tableName
                                    orError:&error];
        if (!error) {
            [self.operationQueue removeOperation:operation orError:&error];
        }
        
        if (completion) {
            [self.callbackQueue addOperationWithBlock:^{
                completion(error);
            }];
        }
    });
}

/// Verify our input is valid and try to pull our data down from the server
- (void) pullWithQuery:(MSQuery *)query queryId:(NSString *)queryId completion:(MSSyncBlock)completion;
{
    // make a copy since we'll be modifying it internally
    MSQuery *queryCopy = [query copy];
    
    // We want to throw on unsupported fields so we can change this decision later
    NSError *error;
    NSDictionary *isDeletedParams = [MSSyncContext dictionary:queryCopy.parameters entriesForCaseInsensitiveKey:@"__includedeleted"];
    if (queryCopy.selectFields) {
        error = [self errorWithDescription:@"Use of selectFields in not supported in pullWithQuery:"
                              andErrorCode:MSInvalidParameter];
    }
    else if (queryCopy.includeTotalCount) {
        error = [self errorWithDescription:@"Use of includeTotalCount is not supported in pullWithQuery:"
                              andErrorCode:MSInvalidParameter];
    }
    else if (queryId && queryCopy.orderBy.count > 0) {
        error = [self errorWithDescription: @"Use of orderBy is not supported when a queryId is specified"
                              andErrorCode:MSInvalidParameter];
    }
    else if (queryId && queryCopy.fetchOffset > 0) {
        error = [self errorWithDescription: @"Use of fetchOffset is not supported when a queryId is specified"
                              andErrorCode:MSInvalidParameter];
    }
    else if ([MSSyncContext dictionary:queryCopy.parameters containsCaseInsensitiveKey:@"__systemproperties"]) {
        error = [self errorWithDescription:@"Use of '__systemProperties' is not supported in pullWithQuery parameters:" andErrorCode:MSInvalidParameter];
    }
    else if (queryCopy.syncTable) {
        // Otherwise we convert the sync table to a normal table
        queryCopy.table = [[MSTable alloc] initWithName:queryCopy.syncTable.name client:queryCopy.syncTable.client];
        queryCopy.syncTable = nil;
    }
    else if (!queryCopy.table) {
        // MSQuery itself should disallow this, but for safety verify we have a table object
        error = [self errorWithDescription:@"Missing required syncTable object in query"
                              andErrorCode:MSInvalidParameter];
    }
    
    if (!error && isDeletedParams.count > 0) {
        // if there are any __includeDeleted params set to NO we want to throw because we would overwrite them
        for (NSNumber *value in isDeletedParams.allValues) {
            if (!value.boolValue) {
                error = [self errorWithDescription:@"The '__includeDeleted' parameter value must be YES if used for pullWithQuery:"
                                      andErrorCode:MSInvalidParameter];
                break;
            }
        }
    }
    
    // Return error if possible, return on calling
    if (error) {
        if (completion) {
            completion(error);
        }
        return;
    }
    
    // Get the required system properties from the Store
    if ([self.dataSource respondsToSelector:@selector(systemPropetiesForTable:)]) {
        queryCopy.table.systemProperties = [self.dataSource systemPropetiesForTable:queryCopy.table.name];
    } else {
        queryCopy.table.systemProperties = MSSystemPropertyVersion;
    }
    
    // add __includeDeleted
    if (!queryCopy.parameters) {
        queryCopy.parameters = @{@"__includeDeleted" : @YES};
    } else {
        NSMutableDictionary *mutableParameters = [queryCopy.parameters mutableCopy];
        [mutableParameters setObject:@YES forKey:@"__includeDeleted"];
        queryCopy.parameters = mutableParameters;
    }
    
    queryCopy.table.systemProperties |= MSSystemPropertyDeleted;
    
    if (queryId) {
        queryCopy.table.systemProperties |= MSSystemPropertyUpdatedAt;
        NSSortDescriptor *orderByUpdatedAt = [NSSortDescriptor sortDescriptorWithKey:MSSystemColumnUpdatedAt ascending:YES];
        queryCopy.orderBy = [NSArray arrayWithObject:orderByUpdatedAt];
    }
    
    // Begin the actual pull request
    [self pullWithQueryInternal:queryCopy queryId:queryId completion:completion];
}

/// Basic pull logic is:
///  Check if our table has pending operations, if so, push
///    If push fails, return error, else repeat while we have pending operations
///  Read from server and get the new results
///  If our table became dirty while we read from the server, start over
///  Else save our data into the local store
- (void) pullWithQueryInternal:(MSQuery *)query queryId:(NSString *)queryId completion:(MSSyncBlock)completion
{
    dispatch_async(writeOperationQueue, ^{
        // Before we can pull from the remote, we need to make sure out table doesn't having pending operations
        NSArray *tableOps = [self.operationQueue getOperationsForTable:query.table.name item:nil];
        if (tableOps.count > 0) {
            [self pushWithCompletion:^(NSError *error) {
                // For now we just abort the pull if the push failed to complete successfully
                // Long term we can be smarter and check if our table succeeded
                if (error) {
                    if (completion) {
                        completion(error);
                    }
                }
                else {
                    // Check again if we have new pending ops while we synced, and repeat as needed
                    [self pullWithQueryInternal:query queryId:queryId completion:completion];
                }
            }];
            return;
        }
        else {
            // TODO: Allow users to cancel operations
            MSQueuePullOperation *pull = [[MSQueuePullOperation alloc] initWithSyncContext:self
                                                                                     query:query
                                                                                   queryId:queryId
                                                                             dispatchQueue:writeOperationQueue
                                                                             callbackQueue:self.callbackQueue
                                                                                completion:completion];
            
            
            [pushQueue_ addOperation:pull];
        }
    });
}

/// In order to purge data from the local store, purge first checks if there are any pending operations for
/// the specific table on the query. If there are, no purge is performed and an error returned to the user.
/// Otherwise clear the local table of all macthing records
- (void) purgeWithQuery:(MSQuery *)query queryId:(NSString *)queryId force:(BOOL)force completion:(MSSyncBlock)completion
{
    // purge needs exclusive access to the storage layer
    dispatch_async(writeOperationQueue, ^{
        NSError *error;
        
        // purge the queryId, if specified
        if (queryId) {
            NSPredicate *predicate = [NSPredicate predicateWithFormat:@"table == %@ && key == %@ && keyType == %ld", query.syncTable.name, queryId, MSConfigKeyDeltaToken];
            MSSyncTable *configTable = [[MSSyncTable alloc] initWithName:self.dataSource.configTableName client:query.syncTable.client];
            MSQuery *query = [[MSQuery alloc] initWithSyncTable:configTable predicate:predicate];
            [self.dataSource deleteUsingQuery:query orError:&error];
        }
        
        if (!error) {
            // Check if our table is dirty
            NSArray *tableOps = [self.operationQueue getOperationsForTable:query.syncTable.name item:nil];
            
            if (tableOps.count > 0) {
                if (query.predicate || !force) {
                    error = [self errorWithDescription:@"The table cannot be purged because it has pending operations"
                                          andErrorCode:MSPurgeAbortedPendingChanges];
                }
                
                if (!error) {
                    // delete operations one-by-one, which will delete any errors
                    for (int i = 0; i < tableOps.count; i++) {
                        [self.operationQueue removeOperation:tableOps[i] orError:&error];
                    }
                }
            }
        }
        
        if (!error) {
            // We can safely delete all items on this table (no pending operations)
            [self.dataSource deleteUsingQuery:query orError:&error];
        }
        
        if (completion) {
            [self.callbackQueue addOperationWithBlock:^{
                completion(error);
            }];
        }
    });
}

+ (BOOL) dictionary:(NSDictionary *)dictionary containsCaseInsensitiveKey:(NSString *)key
{
    for (NSString *object in dictionary.allKeys) {
        if ([object caseInsensitiveCompare:key] == NSOrderedSame) {
            return YES;
        }
    }
    return NO;
}

+ (NSDictionary *) dictionary:(NSDictionary *)dictionary entriesForCaseInsensitiveKey:(NSString *)key
{
    NSMutableDictionary *matches = [NSMutableDictionary dictionary];
    for (NSString *object in dictionary.allKeys) {
        if ([object caseInsensitiveCompare:key] == NSOrderedSame) {
            [matches setValue:dictionary[object] forKey:object];
        }
    }
    return matches;
}

# pragma mark * NSError helpers


-(NSError *) errorWithDescription:(NSString *)description
                     andErrorCode:(NSInteger)errorCode
{
    NSDictionary *userInfo = @{ NSLocalizedDescriptionKey: description };
    
    return [NSError errorWithDomain:MSErrorDomain
                               code:errorCode
                           userInfo:userInfo];
}


@end
