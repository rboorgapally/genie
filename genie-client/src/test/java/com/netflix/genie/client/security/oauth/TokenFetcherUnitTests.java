/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.client.security.oauth;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit Tests for the Token Fetcher class.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class TokenFetcherUnitTests {

    private static final String URL = "http://url";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final String SCOPE = "scope";

    /**
     * Test the constructor with URL missing.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testConstructorWithEmptyUrl() throws GenieException {
        new TokenFetcher(null, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, SCOPE);
    }

    /**
     * Test the constructor with client id missing.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testConstructorWithEmptyClientIdl() throws GenieException {
        new TokenFetcher(URL, null, CLIENT_SECRET, GRANT_TYPE, SCOPE);
    }

    /**
     * Test the constructor with client secret missing.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testConstructorWithEmptyClientSecret() throws GenieException {
        new TokenFetcher(URL, CLIENT_ID, null, GRANT_TYPE, SCOPE);
    }

    /**
     * Test the constructor with grant type missing.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testConstructorWithEmptyGrantType() throws GenieException {
        new TokenFetcher(URL, CLIENT_ID, CLIENT_SECRET, null, SCOPE);
    }

    /**
     * Test the constructor with scope missing.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testConstructorWithEmptyScope() throws GenieException {
        new TokenFetcher(URL, CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, null);
    }

    /**
     * Test the constructor with malformed url.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieException.class)
    public void testWithMalformedUrl() throws GenieException {
        new TokenFetcher("foo", CLIENT_ID, CLIENT_SECRET, GRANT_TYPE, SCOPE);
    }
}
