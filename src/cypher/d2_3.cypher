CALL gds.louvain.mutate('graph', { mutateProperty: 'communityId' })
YIELD communityCount, modularity, modularities