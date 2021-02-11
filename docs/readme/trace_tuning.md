#Scaling and Tuning

Data Prepper for Trace Analytics in v0.7.x supports only vertical scaling. This is because the pipeline setup of Data Prepper uses stateful Service-Map processing. For this Service-Map processing to work Data Prepper needs to receive all span related to a single trace workflow to the same host. 

In the next release we will support horizontal scaling through peer-forwarder plugin, which will route spans belonging to single trace workflow to the same host consistently using hashing techniques. 


## Scaling Tips

We would like to provide the users with some useful tips for scaling the v0.7.x version of Trace Analytics.

[TBD, Target 02/12/2021]