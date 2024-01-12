# About
This library is used to mock a kafka server directly in Go for the purposes
of unit testing. While it provides the ability to use a real kafka client
against the mock server, it does not have nearly all of the capabilities
of the official kafka server.

I find it is easier to maintain code this way, rather than place a facade
in front of the kafka client and mock the client.
