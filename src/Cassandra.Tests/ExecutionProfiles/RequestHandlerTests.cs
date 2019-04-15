// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tests.Connections;
using Cassandra.Tests.Requests;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class RequestHandlerTests
    {
        private long retryCount = 0;

        [Test]
        public async Task task()
        {
            var lbp = new FakeLoadBalancingPolicy();
            var sep = new FakeSpeculativeExecutionPolicy();
            var rp = new FakeRetryPolicy();
            var profile = ExecutionProfile.Builder()
                                          .WithConsistencyLevel(ConsistencyLevel.All)
                                          .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                          .WithReadTimeoutMillis(50)
                                          .WithLoadBalancingPolicy(lbp)
                                          .WithSpeculativeExecutionPolicy(sep)
                                          .WithRetryPolicy(rp)
                                          .Build();

            var mockResult = BuildRequestHandler(
                new SimpleStatement("select").SetIdempotence(true),
                builder =>
                {
                    builder.QueryOptions = 
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                }, 
                profile);

            await mockResult.RequestHandler.SendAsync().ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            Assert.GreaterOrEqual(results.Length, 1);
            var generatedRequest = results[0].Request as QueryRequest;
            Assert.IsNotNull(generatedRequest);
            Assert.AreEqual(ConsistencyLevel.All, generatedRequest.Consistency);
            Assert.AreEqual(ConsistencyLevel.Serial, generatedRequest.SerialConsistency);
            Assert.AreEqual(50, results[0].TimeoutMillis);
            Assert.Greater(Interlocked.Read(ref lbp.count), 0);
            Assert.Greater(Interlocked.Read(ref sep.count), 0);
            Assert.Greater(Interlocked.Read(ref rp.count), 0);
        }

        private RequestHandlerMockResult BuildRequestHandler(
            IStatement statement, 
            Action<TestConfigurationBuilder> configBuilderAct, 
            ExecutionProfile profile)
        {
            var connection = Mock.Of<IConnection>();

            // create config
            var configBuilder = new TestConfigurationBuilder
            {
                ConnectionFactory = new FakeConnectionFactory(() => connection),
                Policies = new Policies(new RoundRobinPolicy(), new ConstantReconnectionPolicy(100), new DefaultRetryPolicy())
            };
            configBuilderAct(configBuilder);
            var config = configBuilder.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock).Setup(i => i.ContactPoints).Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042)});
            Mock.Get(initializerMock).Setup(i => i.GetConfiguration()).Returns(config);

            // create cluster
            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            config.Policies.LoadBalancingPolicy.Initialize(cluster);

            // create session
            var session = new Session(cluster, config, null, Serializer.Default);

            // create request handler
            var requestHandler = new RequestHandler(
                session,
                Serializer.Default,
                statement,
                new RequestOptions(profile, config.Policies, config.SocketOptions, config.QueryOptions, config.ClientOptions));

            // create mock result object
            var mockResult = new RequestHandlerMockResult(requestHandler);

            // mock connection send
            Mock.Get(connection)
                .Setup(c => c.Send(It.IsAny<IRequest>(), It.IsAny<Action<Exception, Response>>(), It.IsAny<int>()))
                .Returns<IRequest, Action<Exception, Response>, int>((req, act, timeout) =>
                {
                    mockResult.SendResults.Enqueue(new ConnectionSendResult { Request = req, TimeoutMillis = timeout });
                    Task.Run(async () =>
                    {
                        if (Interlocked.Read(ref retryCount) > 0)
                        {
                            await Task.Delay(1).ConfigureAwait(false);
                            act(null, new ProxyResultResponse(ResultResponse.ResultResponseKind.Void));
                        }
                        else
                        {
                            await Task.Delay(500).ConfigureAwait(false);
                            act(new OverloadedException(string.Empty), null);
                        }
                        Interlocked.Increment(ref retryCount);
                    });
                    return new OperationState(act)
                    {
                        Request = req,
                        TimeoutMillis = timeout
                    };
                });
            Mock.Get(connection)
                .SetupGet(c => c.Address)
                .Returns(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042));

            return mockResult;
        }

        private class ConnectionSendResult
        {
            public IRequest Request { get; set; }

            public int TimeoutMillis { get; set; }
        }

        private class RequestHandlerMockResult
        {
            public RequestHandlerMockResult(IRequestHandler requestHandler)
            {
                RequestHandler = requestHandler;
            }

            public IRequestHandler RequestHandler { get; }

            public ConcurrentQueue<ConnectionSendResult> SendResults { get; } = new ConcurrentQueue<ConnectionSendResult>();
        }

        private class FakeLoadBalancingPolicy : ILoadBalancingPolicy
        {
            public long count;

            public void Initialize(ICluster cluster)
            {
            }

            public HostDistance Distance(Host host)
            {
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                Interlocked.Increment(ref count);
                return new List<Host> { new Host(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042)) };
            }
        }

        private class FakeRetryPolicy : IExtendedRetryPolicy
        {
            public long count;

            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                Interlocked.Increment(ref count);
                if (Interlocked.Read(ref count) > 1)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(cl);
                }
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                Interlocked.Increment(ref count);
                if (Interlocked.Read(ref count) > 1)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(cl);
                }
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                Interlocked.Increment(ref count);
                if (Interlocked.Read(ref count) > 1)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(cl);
                }
            }

            public RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry)
            {
                Interlocked.Increment(ref count);
                if (Interlocked.Read(ref count) > 1)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(null);
                }
            }
        }

        private class FakeSpeculativeExecutionPolicy : ISpeculativeExecutionPolicy
        {
            public long count;

            public void Dispose()
            {
            }

            public void Initialize(ICluster cluster)
            {
            }

            public ISpeculativeExecutionPlan NewPlan(string keyspace, IStatement statement)
            {
                Interlocked.Increment(ref count);
                return new ConstantSpeculativeExecutionPolicy(5, 1).NewPlan(keyspace, statement);
            }
        }
    }
}