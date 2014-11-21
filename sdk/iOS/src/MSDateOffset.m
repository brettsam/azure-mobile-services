//
//  NSDate_MSDateOffset.h
//  WindowsAzureMobileServices
//
//  Created by Brett Samblanet on 11/19/14.
//  Copyright (c) 2014 Windows Azure. All rights reserved.
//

#import <Foundation/Foundation.h>
#import "MSDateOffset.h"

@implementation MSDateOffset

@synthesize date = date_;

-(id)initWithDate:(NSDate *)date {
    self.date = date;
    return self;
}

@end
