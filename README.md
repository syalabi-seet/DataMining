# DataMining

## Status
```
Bloomberg
- 500,000 cells (per security-field) daily limit (best case) or 500,000 cells monthly limit (worst case)
- 97,000+ tickers
- 3 terminals
- 3 fields, 40 quarters for BDH call, xx fields for BDP call
- (97000 x 3 x 40) = 11,640,000 cells for BDH calls, xx cells for BDP call
- (11640000 / (500000 * 3)) = 7.76 days (best case), 7.76 months (worst case)
- Alternatives:
    - Bulk data using Bloomberg Data
    - Partial data from Endowment department and topping up to get historical data

Refinitiv
- 5,000 row limit via excel addon
- Not all ESG fields available through data API, but all ESG fields available using bulk API
- Alternatives:
    - Obtain access to additional scope for bulk API calls via Refinitiv Account Manager
```

## Scopes needed
https://developers.refinitiv.com/en/api-catalog/refinitiv-data-platform/refinitiv-data-platform-apis/tutorials/introductory-tutorials/authorization-all-about-tokens
```
trapi.data.esg.read
trapi.data.esg.metadata.read
trapi.data.esg.universe.read
trapi.data.esg.views-basic.read
trapi.data.esg.views-scores.read
trapi.data.esg.views-scores-standard.read
trapi.data.esg.views-scores-full.read
trapi.data.esg.views-measures.read
trapi.data.esg.views-measures-standard.read
trapi.data.esg.views-measures-full.read
trapi.data.esg.bulk.read
```

### Not needed
```
trapi.data.api.test
trapi.data.symbology.read
trapi.data.symbology.bulk.read
trapi.data.research.read
trapi.alerts.research.crud
trapi.alerts.news.crud
trapi.auth.cloud-credentials
trapi.cfs.publisher.read
trapi.cfs.publisher.write
trapi.cfs.publisher.setup.write
trapi.cfs.publisher.stream.write
trapi.cfs.subscriber.read
trapi.cfs.claimcheck.write
trapi.cfs.claimcheck.read
trapi.data.news.read
trapi.data.pricing.read
trapi.streaming.pricing.read
trapi.data.historical-pricing.read
trapi.data.quantitative-analytics.read
```

### Available
```
'trapi.alerts.publication.crud', 
'trapi.data.get.data.read', 
'trapi.data.historical-pricing.summaries.read', 
'trapi.alerts.history.crud', 
'trapi.auth.cloud-credentials', 
'trapi.user-framework.application-metadata.raplib', 
'trapi.user-framework.recently-used.crud', 
'trapi.streaming.pricing.read', 
'trapi.searchcore.read', 
'trapi.synthetic.crud', 
'trapi.search.metadata.read', 
'trapi.streaming.prcperf.read', 
'trapi.alerts.preferences.crud', 
'trapi.searchcore.metadata.read', 
'trapi.data.quantitative-analytics.read', 
'trapi.data.historical-pricing.events.read', 
'trapi.frtb.sentimarization', 
'trapi.search.explore.read', 
'trapi.user-framework.mobile.crud', 
'trapi.graphql.subscriber.access', 
'trapi.streaming.synthetic.read', 
'trapi.search.lookup.read', 
'trapi.searchcore.lookup.read', 
'trapi.data.average-volume-analytics.ava_read', 
'trapi.alerts.subscription.crud', 
'trapi.sdbold', 
'trapi.search.read', 
'trapi.user-framework.workspace.crud'
```