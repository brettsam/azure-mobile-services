// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

#import "MSQuery.h"
#import "MSSyncTable.h"
#import "MSClientInternal.h"
#import "MSTableOperation.h"
#import "MSSyncContextInternal.h"

@implementation MSSyncTable

@synthesize client = client_;
@synthesize name = name_;


#pragma mark * Public Initializer Methods


-(id) initWithName:(NSString *)tableName client:(MSClient *)client;
{
    self = [super init];
    if (self)
    {
        client_ = client;
        name_ = tableName;
    }
    return self;
}


#pragma mark * Public Insert, Update, Delete Methods


-(void)insert:(NSDictionary *)item completion:(MSSyncItemBlock)completion
{
    [self.client.syncContext syncTable:self.name item:item action:MSTableOperationInsert completion:completion];
}

-(void)update:(NSDictionary *)item completion:(MSSyncBlock)completion
{
    [self.client.syncContext syncTable:self.name item:item action:MSTableOperationUpdate completion:^(NSDictionary *item, NSError *error) {
        if (completion) {
            completion(error);
        }
    }];
}

-(void)delete:(NSDictionary *)item completion:(MSSyncBlock)completion
{
    [self.client.syncContext syncTable:self.name item:item action:MSTableOperationDelete completion:^(NSDictionary *item, NSError *error) {
        if (completion) {
            completion(error);
        }
    }];
}


#pragma mark * Public Local Storage Management commands


-(void)pullWithQuery:(MSQuery *)query queryId:(NSString *)queryId completion:(MSSyncBlock)completion
{
    if (![self validateQueryId:queryId]) {
        NSDictionary *userInfo = @{ NSLocalizedDescriptionKey:@"Only alphanumeric characters, underscores (_) and dashes (-) are allowed in a queryId" };
        NSError *error = [NSError errorWithDomain:MSErrorDomain
                                             code:MSInvalidQueryId
                                         userInfo:userInfo];
        
        completion(error);
    }
    [self.client.syncContext pullWithQuery:query queryId:queryId completion:completion];
}

-(void)purgeWithQuery:(MSQuery *)query completion:(MSSyncBlock)completion
{
    // If no query, purge all records in the table by default
    if (query == nil) {
        MSQuery *allRecords = [[MSQuery alloc] initWithSyncTable:self];
        [self.client.syncContext purgeWithQuery:allRecords completion:completion];
        
    } else {
        [self.client.syncContext purgeWithQuery:query completion:completion];
    }
}

-(BOOL)validateQueryId:(NSString *)queryId {
    // nil queryIds are allowed
    if (!queryId) {
        return YES;
    }
    NSString *pattern = @"^[a-zA-Z][a-zA-Z0-9_-]{0,24}$";
    NSError *error;
    NSRegularExpression *regex = [NSRegularExpression regularExpressionWithPattern:pattern options:0 error:&error];
    if (error) {
        return NO;
    }
    NSRange range = NSMakeRange(0, queryId.length);
    NSArray *matches = [regex matchesInString:queryId options:0 range:range];
    if (matches.count == 0) {
        return NO;
    }
    return YES;
}

#pragma mark * Public Read Methods


-(void)readWithId:(NSString *)itemId completion:(MSItemBlock)completion
{
    NSError *error;
    NSDictionary *item = [self.client.syncContext syncTable:self.name readWithId:itemId orError:&error];
    if (completion) {
        completion(item, error);
    }
}

-(void)readWithCompletion:(MSReadQueryBlock)completion
{
    MSQuery *query = [[MSQuery alloc] initWithSyncTable:self];
    [query readWithCompletion:completion];
}

-(void)readWithPredicate:(NSPredicate *)predicate completion:(MSReadQueryBlock)completion
{
    MSQuery *query = [[MSQuery alloc] initWithSyncTable:self predicate:predicate];
    [query readWithCompletion:completion];
}


#pragma mark * Public Query Methods


-(MSQuery *)query {
    return [[MSQuery alloc] initWithSyncTable:self];
}

-(MSQuery *)queryWithPredicate:(NSPredicate *)predicate
{
    return [[MSQuery alloc] initWithSyncTable:self predicate:predicate];
}


@end
