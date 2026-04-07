# Level Zero Garbage Collection

Now that we've shown how to derive a safe-to-GC epoch from the Raft log, we need a system to delete unneeded L0 data from object storage. Ideally, all this system needs is the safe epoch `M` and access to our bucket. So let's walk through some high level design goals for such a system to see how they coalesce into a concrete implementation.

## Stateless: exploiting lexicographic ordering

A natural approach to garbage collection might be to advance a persistent high water mark over time so that the garbage collector is aware of how much progress it's made and where to start working next. There is nothing inherently wrong with this, but it would require an internal topic and potentially a custom state machine to track what is in essence a very small amount of state. Every such structure imposes operation complexity and maintenance burden for future developers.

Cloud topics L0 GC completely avoids this, exploiting properties of the object key namespace and object storage APIs to make progress without any persistent state. Recall the structure of L0 object IDs:

```
level_zero/data/{prefix}/{epoch}/{uuid}
```

Each object's epoch is embedded directly in its name, and the epoch itself is zero padded to 18 decimal digits. All the major object storage providers return bucket LIST results in lexicographic order, so when GC lists objects it naturally sees the lowest epochs first (within a given preifix...more on that later). In this way, the bucket listing itself provides the progress pointer we need. No additional state required.

## 

GC works by issuing LIST requests to a cloud storage bucket, scanning through the results, and issuing DELETE requests for the objects it deems safe to delete. LIST operations have a cost

Each operation carries some cost, whether that's per-request pricing imposed by the cloud provider or eating into rate limits or just competing with other parts of Redpanda for limited resources, we have a number of strategies geared toward keeping 

Recall the numeric
