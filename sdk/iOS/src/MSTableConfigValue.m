// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

#import "MSTableConfigValue.h"
#import "MSJSONSerializer.h"
#import "MSError.h"

@implementation MSTableConfigValue

#pragma mark - Initialization

- (id) initWithSerializedItem:(NSDictionary *)item
{
    self = [super init];
    if (self) {
        _id = [[item objectForKey:@"id"] integerValue];
        _table = [item objectForKey:@"table"];
        _keyType = [[item objectForKey:@"keyType"] integerValue];
        _key = [item objectForKey:@"key"];
        _value = [item objectForKey:@"value"];
    }
    return self;
}

- (NSDictionary *) serialize
{
    return @{ @"id": [NSNumber numberWithInteger:self.id], @"table": self.table, @"keyType": [NSNumber numberWithInteger:self.keyType], @"key": self.key, @"value": self.value };
}

@end


