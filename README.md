## Subscription manager middleware module.

Keeps only the highest necessary subscription active at any given time. If any of the downstream module chains stop sending requests for long enough time they are considered dead and their subscription will be removed from submgr, possibly leading to reduced level of subscription on the tickers that were subscribed on that particular module chain.

submgr is a multiplexing module that combines many connectors into one. Connectors will be prefixed with the connector name (`connector_name` from `get_status` command). This prefix will be prepended on all the ticker ids to specify the appropriate module chain path.
