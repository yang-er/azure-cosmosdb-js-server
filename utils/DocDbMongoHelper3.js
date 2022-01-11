//---------------------------------------------------------------------------------------------------
//
// In this script, keep only scripts that are releated to ETCD Key Value Distributed store.
//
//---------------------------------------------------------------------------------------------------
// Code should run in strict mode wherever possible.
"use strict";
//---------------------------------------------------------------------------------------------------
if (typeof __ != "undefined") {
    // Use 'let' inside a block so that everything defined here is not visible to user script.
    let setupSystemFunctions = function() {
        if (typeof __.sys == "undefined") __.sys = new Object();
        setupSys.call(__.sys);

        //---------------------------------------------------------------------------------------------------
        function setupSys() {
            // Note: set enumerable to true, if you want the function to be enumerated as properties of the __ object.
            Object.defineProperty(this, "kvput", { enumerable: false, configurable: false, writable: false, value: kvput });
            Object.defineProperty(this, "txn", { enumerable: false, configurable: false, writable: false, value: txn });
            Object.defineProperty(this, "revokeLease", { enumerable: false, configurable: false, writable: false, value: revokeLease });
            Object.defineProperty(this, "range", { enumerable: false, configurable: false, writable: false, value: range });
            Object.defineProperty(this, "kvdelete", { enumerable: false, configurable: false, writable: false, value: kvdelete });
            Object.defineProperty(this, "compact", { enumerable: false, configurable: false, writable: false, value: deleteDocuments });
        }

        // --------------------------------------
        // KvPut Stored Procedure
        // --------------------------------------
        function kvput(putRequest) {
            var context = { retry: 0 };
            kvputAsync(putRequest, context);
        }

        async function kvputAsync(putRequest, context) {
            var spResponse = getContext().getResponse();
            try {
                var response = await KeyValueStore.processPutAsync(putRequest);
                spResponse.setBody(response);
            } catch (e) {
                console.log(`putAsync_exception_status= ${e.number} ${e.toString()}`);
                if (context.retry < Constants.retryCount && (e.number == ApiStatusCodes.PreconditionFailure || e.number == ApiStatusCodes.Conflict || e.number == ApiStatusCodes.NotFound)) {
                    context.retry++;
                    console.log(`putAsync_exception_Retry_${context.retry} ${e.toString()}`);
                    return await kvputAsync(putRequest, context);
                }
                getContext().abort(e);
            }
        }

        // --------------------------------------
        // KvDelete Stored Procedure
        // --------------------------------------
        function kvdelete(request) {
            var context = { retry: 0 };
            kvdeleteAsync(request, context);
        }

        async function kvdeleteAsync(request, context) {
            var spResponse = getContext().getResponse();
            try {
                var response = await KeyValueStore.processDeleteAsync(request);
                spResponse.setBody(response);
            } catch (e) {
                console.log(`deleteAsync_exception_status= ${e.number} ${e.toString()}`);
                if (context.retry < Constants.retryCount && (e.number == ApiStatusCodes.PreconditionFailure || e.number == ApiStatusCodes.Conflict || e.number == ApiStatusCodes.NotFound)) {
                    context.retry++;
                    console.log(`deleteAsync_exception_Retry_${context.retry} ${e.toString()}`);
                    return await KeyValueStore.processDeleteAsync(request);
                }
                getContext().abort(e);
            }
        }

        // --------------------------------------
        // Range Stored Procedure
        // --------------------------------------
        function range(request) {
            rangeAsync(request);
        }

        async function rangeAsync(request) {
            var spResponse = getContext().getResponse();
            try {
                var response = await KeyValueStore.processRangeAsync(request);
                spResponse.setBody(response);
            } catch (e) {
                console.log(`rangeAsync_exception_status= ${e.number} ${e.toString()}`);
                getContext().abort(e);
            }
        }

        // -------------------------------------------------------------------------------------------------
        // Compaction Stored Procedure - Etcd stored proc that deletes the given documents from the database
        // parameters: array of self link of documents that need to be deleted. 
        // -------------------------------------------------------------------------------------------------
        function deleteDocuments(documentsToDeleteSelfLinks) {
            // Validate input.
            if (!documentsToDeleteSelfLinks || !Array.isArray(documentsToDeleteSelfLinks)) throw "Incorrect input";
            if (!documentsToDeleteSelfLinks.length) throw "No documents provided";

            var responseBody = {
                deletedDocs: 0
            };

            deleteDocumentsAsync(documentsToDeleteSelfLinks);

            async function deleteDocumentsAsync(documentsToDeleteSelfLinks) {
                var response = getContext().getResponse();

                try {
                    for (let docSelfLink of documentsToDeleteSelfLinks) {
                        await deleteDocumentAsync(docSelfLink);
                        responseBody.deletedDocs++;
                    }
                    response.setBody(responseBody);
                }
                catch (e) {
                    if (e.message === GrpcError.ErrAsyncNotAccepted.detail) {
                        response.setBody(responseBody);
                        return;
                    }
                    console.log(`deleteDocumentsAsync_exception_status= ${e.number} ${e.toString()}`);
                    getContext().abort(e);
                }
            }
            
        }

        // --------------------------------------
        // Revoke Lease Stored Procedure
        // --------------------------------------
        function revokeLease(lease) {
            revokeLeaseAsync(lease);
        }

        async function revokeLeaseAsync(lease) {
            var spResponse = getContext().getResponse();
            try {
                var response = await KeyValueStore.processRevokeLeaseAsync(lease);
                spResponse.setBody(response);
            } catch (e) {
                console.log(`revokeLeaseAsync_exception_status= ${e.number} ${e.toString()}`);
                getContext().abort(e);
            }
        }

        // --------------------------------------
        // Txn Stored Procedure
        // --------------------------------------
        function txn(request) {
            var context = { retry: 0 };
            txnAsync(request, context);
        }

        async function txnAsync(request, context) {
            var response = getContext().getResponse();
            try {
                var txnResponse = await KeyValueStore.processTxnAsync(request);
                response.setBody(txnResponse);
            } catch (e) {
                console.log(`txnAsync_exception_status= ${e.number} ${e.toString()}`);
                if (context.retry < Constants.retryCount && (e.number == ApiStatusCodes.PreconditionFailure || e.number == ApiStatusCodes.Conflict || e.number == ApiStatusCodes.NotFound)) {
                    context.retry++;
                    console.log(`txnAsync_exception_Retry_${context.retry} ${e.toString()}`);
                    return await txnAsync(request, context);
                }
                console.log(`txnAsync_exception_status= ${e.number} ${e.toString()}`);
                getContext().abort(e);
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        /// Enums
        ////////////////////////////////////////////////////////////////////////////
        var CompareResult;
        (function(CompareResult) {
            CompareResult[CompareResult["equal"] = 0] = "equal";
            CompareResult[CompareResult["greater"] = 1] = "greater";
            CompareResult[CompareResult["less"] = 2] = "less";
            CompareResult[CompareResult["not_equal"] = 3] = "not_equal";
        })(CompareResult || (CompareResult = {}));
        var CompareTarget;
        (function(CompareTarget) {
            CompareTarget[CompareTarget["version"] = 0] = "version";
            CompareTarget[CompareTarget["create"] = 1] = "create";
            CompareTarget[CompareTarget["mod"] = 2] = "mod";
            CompareTarget[CompareTarget["value"] = 3] = "value";
            CompareTarget[CompareTarget["lease"] = 4] = "lease";
        })(CompareTarget || (CompareTarget = {}));
        var OpCase;
        (function(OpCase) {
            OpCase[OpCase["None"] = 0] = "None";
            OpCase[OpCase["RequestRange"] = 1] = "RequestRange";
            OpCase[OpCase["RequestPut"] = 2] = "RequestPut";
            OpCase[OpCase["RequestDeleteRange"] = 3] = "RequestDeleteRange";
            OpCase[OpCase["RequestTxn"] = 4] = "RequestTxn";
        })(OpCase || (OpCase = {}));
        var RangeRequestSortOrder;
        (function(RangeRequestSortOrder) {
            RangeRequestSortOrder[RangeRequestSortOrder["None"] = 0] = "None";
            RangeRequestSortOrder[RangeRequestSortOrder["Ascend"] = 1] = "Ascend";
            RangeRequestSortOrder[RangeRequestSortOrder["Descend"] = 2] = "Descend";
        })(RangeRequestSortOrder || (RangeRequestSortOrder = {}));
        var RangeRequestSortTarget;
        (function(RangeRequestSortTarget) {
            RangeRequestSortTarget[RangeRequestSortTarget["Key"] = 0] = "Key";
            RangeRequestSortTarget[RangeRequestSortTarget["Version"] = 1] = "Version";
            RangeRequestSortTarget[RangeRequestSortTarget["Create"] = 2] = "Create";
            RangeRequestSortTarget[RangeRequestSortTarget["Mod"] = 3] = "Mod";
            RangeRequestSortTarget[RangeRequestSortTarget["Value"] = 4] = "Value";
        })(RangeRequestSortTarget || (RangeRequestSortTarget = {}));
        var EtcdDocumentType;
        (function(EtcdDocumentType) {
            EtcdDocumentType[EtcdDocumentType["keyvalue"] = 0] = "keyvalue";
            EtcdDocumentType[EtcdDocumentType["putmvcc"] = 1] = "putmvcc";
            EtcdDocumentType[EtcdDocumentType["deletemvcc"] = 2] = "deletemvcc";
            EtcdDocumentType[EtcdDocumentType["lease"] = 3] = "lease";
            EtcdDocumentType[EtcdDocumentType["system"] = 4] = "system";
        })(EtcdDocumentType || (EtcdDocumentType = {}));
        ////////////////////////////////////////////////////////////////////////////
        /// Json Contants
        ////////////////////////////////////////////////////////////////////////////
        var JsonConstants = {
            bytestringZero: "00",
            Key: "key",
            Lease: "lease",
            Value: "value",
            Type: "type",
            Version: "version",
            Lsn: "_lsn",
            CreateRev: "createrev"
        };
        var Constants = {
            retryCount: 4
        };
        var ApiStatusCodes = {
            NotFound: 404,
            Conflict: 409,
            PreconditionFailure: 412
        };
        ////////////////////////////////////////////////////////////////////////////
        /// KeyValue Store
        ////////////////////////////////////////////////////////////////////////////
        class KeyValueStore {
            //
            // Processes a put request.
            // parameters:
            //  - request: an object deserialized from EtcdPutRequest.
            // returns:
            //    An object in the form of EtcdPutResponse.
            //
            static async processPutAsync(request) {
                var response = {};

                if (!request.ignorelease) {
                    if (!(await KeyValueStore.validateLeaseAsync(request.lease))) {
                        response.status = GrpcError.ErrGRPCLeaseNotFound;
                        return response;
                    }
                }

                var query = KeyValueQuery.getSqlQueryForKeys({
                    key: request.key,
                    latestKeysOnly: true
                });
                var results = await queryDocumentsAsync(query, null);
                var existing = results && results[0];

                if (existing) {
                    // this case is when there is an existing version of this key.

                    if (request.isprevkv) {
                        response.prevkv = existing;
                    }

                    // replace with the next version.
                    var next = KeyValue.createNext(existing, request);
                    var ac = { "etag": existing._etag, type: "IfMatch" };
                    await replaceDocumentAsync(next, ac);

                    // create history document.
                    let history = KeyValue.createHistory(next, false);
                    await createDocumentAsync(history);
                } else {
                    // this case is when there is no existing version of this key (but there may be previous history).

                    if (request.ignorelease) {
                        response.status = GrpcError.ErrGRPCKeyNotFound;
                        return response;
                    }
                    if (request.ignorevalue) {
                        response.status = GrpcError.ErrGRPCKeyNotFound;
                        return response;
                    }

                    // create document
                    var kv = KeyValue.createNew(request);
                    await createDocumentAsync(kv);

                    // create history document.
                    let history = KeyValue.createHistory(kv, false);
                    await createDocumentAsync(history);
                }

                return response;
            }

            //
            // Processes a delete request.
            // parameters:
            //  - request: an object deserialized from EtcdDeleteRequest.
            // returns:
            //    An object in the form of EtcdDeleteResponse.
            //
            static async processDeleteAsync(request) {
                var response = { deleted: 0 };
                if (request.isprevkv) {
                    response.prevkvs = new Array();
                }

                var query = KeyValueQuery.getSqlQueryForKeys({
                    key: request.key,
                    rangeEnd: request.rangeend,
                    latestKeysOnly: true
                });
                var results = await queryDocumentsAsync(query, null, true);

                for (let kv of results) {
                    if (request.isprevkv) {
                        response.prevkvs.push(kv);
                    }

                    // delete the object now.
                    await deleteDocumentAsync(kv._self);

                    // create history
                    var history = KeyValue.createHistory(kv, true);
                    await createDocumentAsync(history);

                    response.deleted++;
                }

                return response;
            }

            //
            // Processes a range request.
            // parameters:
            //  - request: an object deserialized from EtcdRangeRequest.
            // returns:
            //    An object in the form of EtcdRangeResponse.
            //
            static async processRangeAsync(request) {
                var response = { count: 0 };

                if (!(await KeyValueStore.validateCompactAsync(request.revision))) {
                    response.status = GrpcError.ErrGRPCCompacted;
                    return response;
                }

                // Default sorting behavior, when the sortOrder is None and SortTarget is Key itself, is ascending order.
                if (request.sortorder === RangeRequestSortOrder.None && request.sorttarget === RangeRequestSortTarget.Key)
                {
                    request.sortorder = RangeRequestSortOrder.Ascend;
                }

                // TODO: support keysOnly projection.

                var queryInfo;
                if (request.revision <= 0) {
                    // query latest version of keys that haven't been deleted.
                    queryInfo = {
                        key: request.key,
                        rangeEnd: request.rangeend,
                        latestKeysOnly: true,
                        countsOnly: request.countsonly,
                        maxModRevision: request.maxmodrevision,
                        minModRevision: request.minmodrevision,
                        maxCreateRevision: request.maxcreaterevision,
                        minCreateRevision: request.mincreaterevision,
                        sortOrder: request.sortorder,
                        sortTarget: request.sorttarget
                    };

                    // if sort by create revision, we cannot accompalish this in query, so we will do sorting in post-processing.
                    if (request.sorttarget === RangeRequestSortTarget.Create) {
                        queryInfo.sortTarget = RangeRequestSortTarget.None;
                    }
                } else {
                    // query historical versions of keys.
                    // for this case, we need to retrieve all historical versions up to that revision then deduce the result from them.
                    // This also means things like countsOnly and sorting are accompalished by post processing in stored proc rather than by query.

                    queryInfo = {
                        key: request.key,
                        rangeEnd: request.rangeend,
                        includeTombstone: true,
                        latestKeysOnly: false,
                        maxModRevision: request.revision
                    };
                }

                var query = KeyValueQuery.getSqlQueryForKeys(queryInfo);
                var documents = await queryDocumentsAsync(query, null, true);

                if (request.revision <= 0) {
                    if (request.countsonly) {
                        // for this case we used COUNT aggregate function in the SQL query, so the count is the 1st element in the output array.
                        response.count = documents[0];
                    } else {
                        // if sort by create revision is requested, our SQL query cannot handle the sorting,
                        // so we need to sort it here.
                        if (request.sortorder !== RangeRequestSortOrder.None &&
                            request.sorttarget === RangeRequestSortTarget.Create) {
                            KeyValueStore.sortDocuments(documents, request.sortorder, request.sorttarget);
                        }

                        // if limit is specified, we should return no more than what the limit is, but the
                        // total count should still indicate total available keys.
                        response.count = documents.length;
                    }
                } else {
                    // historical versions must be processed to deduce the query result at the given point of time.
                    documents = KeyValueStore.processHistoricalVersions(documents);

                    // also need a manual sort.
                    if (request.sortorder !== RangeRequestSortOrder.None && !request.countsonly) {
                        KeyValueStore.sortDocuments(documents, request.sortorder, request.sorttarget);
                    }

                    // TODO: handle min/max create/mod revision filters

                    response.count = documents.length;
                }

                if (!request.countsonly) {
                    response.kvs = new Array();
                    response.more = request.limit > 0 && documents.length > request.limit || false;

                    var countToCopy = request.limit > 0 && documents.length > request.limit
                        ? request.limit
                        : documents.length;
                    for (var i = 0; i < countToCopy; i++) {
                        var kv = documents[i];
                        response.kvs.push(kv);
                    }
                }

                return response;
            }

            //
            // Processes a txn request.
            // parameters:
            //  - request: an object deserialized from EtcdTxnRequest.
            // returns:
            //    An object in the form of EtcdTxnResponse.
            //
            static async processTxnAsync(request) {
                var allCompareEvalSuccess = true;
                for (let compare of request.compare) {
                    if (!(await KeyValueStore.compareAsync(compare))) {
                        allCompareEvalSuccess = false;
                        break;
                    }
                }

                if (allCompareEvalSuccess) {
                    return await KeyValueStore.executeRequestOpAsync(allCompareEvalSuccess, request.success);
                } else {
                    return await KeyValueStore.executeRequestOpAsync(allCompareEvalSuccess, request.failure);
                }
            }

            //
            // Processes a revoke lease request
            // parameters:
            //  - lease: the lease id to revoke.
            // returns:
            //    The revoke lease response.
            //
            static async processRevokeLeaseAsync(lease) {
                var response = {};

                // delete lease object
                var leaseQuery = `SELECT * FROM c WHERE c.lease = '${lease}' AND c.type = ${EtcdDocumentType.lease}`;
                var leaseDocuments = await queryDocumentsAsync(leaseQuery, null);
                if (leaseDocuments.length === 0) {
                    response.status = GrpcError.ErrGRPCLeaseNotFound;
                    return response;
                }
                await deleteDocumentAsync(leaseDocuments[0]._self);

                // delete keyvalue documents associated with this lease.
                var keyValueQuery = KeyValueQuery.getSqlQueryForKeys({
                    lease: lease,
                    latestKeysOnly: true
                });
                var keyValueDocuments = await queryDocumentsAsync(keyValueQuery, null);

                for (let kv of keyValueDocuments) {
                    // delete the keyvalue
                    await deleteDocumentAsync(kv._self);

                    // create history
                    var history = KeyValue.createHistory(kv, true);
                    await createDocumentAsync(history);
                }

                return response;
            }

            static sortDocuments(documents, sortOrder, sortTarget) {
                if (documents && documents.length) {
                    documents.sort((left, right) => {
                        var leftValue, rightValue;

                        if (sortTarget === RangeRequestSortTarget.Key) {
                            leftValue = left && left.key;
                            rightValue = right && right.key;
                        } else if (sortTarget === RangeRequestSortTarget.Mod) {
                            leftValue = left && left._lsn;
                            rightValue = right && right._lsn;
                        } else if (sortTarget === RangeRequestSortTarget.Value) {
                            leftValue = left && left.value;
                            rightValue = right && right.value;
                        } else if (sortTarget === RangeRequestSortTarget.Version) {
                            leftValue = left && left.version;
                            rightValue = right && right.version;
                        } else if (sortTarget === RangeRequestSortTarget.Create) {
                            if (left && left.version === 1) {
                                leftValue = left._lsn;
                            } else {
                                leftValue = left && left.createrev;
                            }

                            if (right && right.version === 1) {
                                rightValue = right._lsn;
                            } else {
                                rightValue = right && right.createrev;
                            }
                        }

                        if (leftValue === rightValue) {
                            return 0;
                        } else {
                            if (leftValue < rightValue) {
                                return sortOrder === RangeRequestSortOrder.Ascend ? -1 : 1;
                            } else {
                                return sortOrder === RangeRequestSortOrder.Ascend ? 1 : -1;
                            }
                        }
                    });
                }

                return documents;
            }

            static processHistoricalVersions(documents) {
                // what we have here are the historical versions for some key range. we need to find the operation
                // with the highest revision for each key to determine its state at the end.
                var lastRevisionMap = {};
                for (let item of documents) {
                    var last = lastRevisionMap[item.key];
                    if (!last || item._lsn > last._lsn) {
                        lastRevisionMap[item.key] = item;
                    }
                }

                // leave out Deletemvcc in the final results, since those keys have been deleted at this snapshot.
                return Object.values(lastRevisionMap).filter(kv => kv.type === EtcdDocumentType.putmvcc);
            }

            static async validateCompactAsync(compactRevision) {
                if (compactRevision <= 0) {
                    return true;
                }

                var query = `SELECT TOP 1 * FROM c WHERE c.type = ${EtcdDocumentType.system}`;
                var results = await queryDocumentsAsync(query, null);
                if (results.length > 0 && compactRevision < results[0].compactto) {
                    return false;
                }
                return true;
            }

            static async validateLeaseAsync(leaseId) {
                if (leaseId == null) {
                    throw "LeaseId is null";
                }

                if (leaseId === "0") {
                    return true;
                }

                var query = `SELECT VALUE COUNT(1) FROM c WHERE c.lease = '${leaseId}' AND c.type = ${EtcdDocumentType.lease} `;
                var results = await queryDocumentsAsync(query, null);

                return results[0] > 0;
            }

            static async compareAsync(compare) {
                var query = KeyValueQuery.getSqlQueryForKeys({
                    key: compare.key,
                    latestKeysOnly: true
                });
                var results = await queryDocumentsAsync(query, null);

                var item = results.length > 0
                    ? results[0]
                    :
                    // the default value if we can't find the items.
                    { createrev: 0, _lsn: 0, version: 0, lease: "0" };

                return KeyValueStore.createEvalFromCompare(item, compare);
            }

            static async executeRequestOpAsync(compareRes, requestOps) {
                var result = { responses: new Array(), succeeded: compareRes };
                for (let rop of requestOps) {
                    switch (rop.case) {
                    case OpCase.RequestPut:
                        result.responses.push({
                            case: OpCase.RequestPut,
                            putresponse: await KeyValueStore.processPutAsync(rop.putrequest),
                        });
                        break;
                    case OpCase.RequestDeleteRange:
                        result.responses.push({
                            case: OpCase.RequestDeleteRange,
                            deleterangeresponse: await KeyValueStore.processDeleteAsync(rop.deleterangerequest),
                        });
                        break;
                    case OpCase.RequestRange:
                        result.responses.push({
                            case: OpCase.RequestRange,
                            rangeresponse: await KeyValueStore.processRangeAsync(rop.rangerequest),
                        });
                        break;
                    case OpCase.RequestTxn:
                        result.responses.push({
                            case: OpCase.RequestTxn,
                            txnresponse: await KeyValueStore.processTxnAsync(rop.txnrequest),
                        });
                        break;
                    default:
                        throw `txnExecuteRequestOpAsync, unknown op case ${rop.case.toString()}`;
                    }
                }
                return result;
            }

            static createEvalFromCompare(item, compare) {
                var left, right;
                switch (compare.target) {
                case CompareTarget.create:
                    if (item.version === 1) {
                        left = item._lsn;
                    } else {
                        left = item.createrev;
                    }
                    right = compare.targetlong;
                    break;
                case CompareTarget.mod:
                    left = item._lsn;
                    right = compare.targetlong;
                    break;
                case CompareTarget.value:
                    left = item.value;
                    right = compare.targetstring;
                    break;
                case CompareTarget.version:
                    left = item.version;
                    right = compare.targetlong;
                    break;
                case CompareTarget.lease:
                    left = item.lease;
                    right = compare.targetstring;
                    break;
                default:
                    throw `txnCreateEvalFromCompare, unknown compare_target ${compare.target.toString()}`;
                }
                switch (compare.result) {
                case CompareResult.equal:
                    return left === right;
                case CompareResult.less:
                    return left < right;
                case CompareResult.greater:
                    return left > right;
                case CompareResult.not_equal:
                    return left !== right;
                default:
                    throw `txncreateEvalFromCompare, unknown compare_result ${compare.result.toString()}`;
                }
            }
        }

        class KeyValue {
            constructor() {
            }

            //
            // Creates a new keyvalue document from the put request when the key does not yet exist.
            // parameters:
            //  - request: an object deserialized from EtcdPutRequest.
            // returns:
            //    A new keyvalue document with data from the given put request.
            static createNew(request) {
                var kv = {};
                kv.type = EtcdDocumentType.keyvalue;
                kv.key = request.key;
                kv.generation = generateUUID();
                kv.value = request.value;
                kv.lease = request.lease;
                kv.version = 1;
                kv.id = request.keyid ? request.keyid : `${kv.type}_${kv.key.substring(0, Math.min(30, kv.key.length))}_${StringUtil.HashCode(kv.key)}`;
                return kv;
            }

            //
            // Creates keyvalue document for the next version of a key that already exists.
            // parameters:
            //  - existing: the existing keyvalue document for the latest version of this key.
            //  - request: an object deserialized from EtcdPutRequest.
            // returns:
            //    A keyvalue document representing the new version of this key.
            static createNext(existing, request) {
                var next = KeyValue.shallowCopy(existing);
                if (!request.ignorevalue) {
                    next.value = request.value;
                }
                if (!request.ignorelease) {
                    next.lease = request.lease;
                }
                // if existing version is v1, the keyvalue document has no createrev, so we must set it this time on the v2 document.
                if (existing.version === 1) {
                    next.createrev = existing._lsn;
                }
                // increment the version
                next.version++;
                return next;
            }

            //
            // Creates a history document for the operation.
            // parameters:
            //  - kv: the existing keyvalue document that was updated or deleted.
            //        for update, kv points to the new keyvalue that is going to overwrite the old keyvalue.
            //  - isDeleted: true if this is a delete operation instead of an update operation.
            // returns:
            //    A new Putmvcc or Deletemvcc document representing the history record for this operation.
            static createHistory(kv, isDeleted) {
                var next = KeyValue.shallowCopy(kv);
                if (isDeleted) {
                    next.type = EtcdDocumentType.deletemvcc;
                    next.version++;
                } else {
                    next.type = EtcdDocumentType.putmvcc;
                }

                next.id = next.id + "_" + next.version + "_" + next.generation;
                delete next._self;
                return next;
            }

            //
            // Makes a shallow copy of the given object.
            // parameters:
            //  - src: the object to be copied.
            // returns:
            //    A shallow copy of the given object.
            static shallowCopy(src) {
                return Object.assign({}, src);
            }
        }

        class KeyValueQuery {
            constructor() {
            }

            //
            // Generates a sql query for querying keyvalue documents.
            // parameters:
            //  - queryInfo: an object containing the query info, with the following properties:
            //    * key (string): the hex representation of the key, per standard key range specification syntax of Etcd.
            //    * rangeEnd (string): the hex representation of the key range end, per standard key range specification syntax of Etcd.
            //    * includeTombstone (boolean): specifies if Deletemvcc documents that represent key deletions should also be returned.
            //    * latestKeysOnly (boolean): if true, only latest version of the keys are returned (EtcdDocumentType.keyvalue). 
            //                                otherwise, historical documents matching the criteria are returned.
            //    * countsOnly (boolean): if true, a query for getting the count of documents matching the criteria is returned.
            //    * maxModRevision (number): filters the keys by the given max modified revision.
            //    * minModRevision (number): filters the keys by the given min modified revision.
            //    * maxCreateRevision (number): filters the keys by the given max create revision.
            //    * minCreateRevision (number): filters the keys by the given min create revision.
            //    * sortOrder (number): specifies the sort order. See RangeRequestSortOrder. Default is None (0).
            //    * sortTarget (number): specifies the property used for sorting. See RangeRequestSortTarget. Default is sort by Key, when sortOrder is not None.
            //    * lease (string): specifies the lease id that the keyvalue documents must be associated with.
            // returns:
            //    A new Tomestone document representing the new version of this key.
            static getSqlQueryForKeys(queryInfo) {
                return `${KeyValueQuery.createSelectClause(queryInfo)} FROM c ${
                    KeyValueQuery.createWhereClause(queryInfo)} ${KeyValueQuery.createOrderByClause(queryInfo)}`;
            }

            static createSelectClause(queryInfo) {
                var selectClause;
                if (queryInfo.countsOnly) {
                    selectClause = "SELECT VALUE COUNT(1)";
                } else {
                    selectClause = "SELECT *";
                }

                return selectClause;
            }

            static createWhereClause(queryInfo) {
                var whereClauses = new Array();

                if (queryInfo.rangeEnd) {
                    if (queryInfo.rangeEnd === JsonConstants.bytestringZero) {
                        if (queryInfo.key !== JsonConstants.bytestringZero) {
                            whereClauses.push(`c.key >= '${queryInfo.key}'`);
                        }
                    } else {
                        whereClauses.push(`c.key >= '${queryInfo.key}'`);
                        whereClauses.push(`c.key < '${queryInfo.rangeEnd}'`);
                    }
                } else if (queryInfo.key || queryInfo.key === "") {
                    whereClauses.push(`c.key = '${queryInfo.key}'`);
                }

                if (queryInfo.latestKeysOnly) {
                    whereClauses.push(`c.type = ${EtcdDocumentType.keyvalue}`);
                } else {
                    if (queryInfo.includeTombstone) {
                        whereClauses.push(
                            `(c.type = ${EtcdDocumentType.putmvcc} OR c.type = ${EtcdDocumentType.deletemvcc})`);
                    } else {
                        whereClauses.push(`c.type = ${EtcdDocumentType.putmvcc}`);
                    }
                }

                if (queryInfo.minModRevision > 0) {
                    whereClauses.push(`c._lsn >= ${queryInfo.minModRevision}`);
                }

                if (queryInfo.maxModRevision > 0) {
                    whereClauses.push(`c._lsn <= ${queryInfo.maxModRevision}`);
                }

                if (queryInfo.maxCreateRevision > 0) {
                    whereClauses.push(
                        `((c.createrev <= ${queryInfo.maxCreateRevision} AND c.version > 1) OR (c._lsn <= ${queryInfo
                        .maxCreateRevision} AND c.version = 1))`);
                }

                if (queryInfo.minCreateRevision > 0) {
                    whereClauses.push(
                        `((c.createrev >= ${queryInfo.minCreateRevision} AND c.version > 1) OR (c._lsn >= ${queryInfo
                        .minCreateRevision} AND c.version = 1))`);
                }

                if (queryInfo.lease) {
                    whereClauses.push(`c.lease = '${queryInfo.lease}'`);
                }

                return KeyValueQuery.combineWhereClauses(whereClauses);
            }

            static createOrderByClause(queryInfo) {
                var order = null;
                if (queryInfo.sortOrder === RangeRequestSortOrder.Ascend) {
                    order = "ASC";
                } else if (queryInfo.sortOrder === RangeRequestSortOrder.Descend) {
                    order = "DESC";
                }

                if (order) {
                    // we do not add ORDER BY for the case of sorting by create revision, this is because
                    // the property that exposes create revision is different depending on version:
                    //   - If version=1, create revision is exposed by _lsn.
                    //   - If version>1, create revision is exposed by createrev.
                    // Cosmos DB SQL query current does not support ORDER BY on computed columns.
                    // Hence sort by create revision needs to be handled by the stored proc.
                    var orderField = "";
                    if (queryInfo.sortTarget === RangeRequestSortTarget.Key) {
                        orderField = JsonConstants.Key;
                    } else if (queryInfo.sortTarget === RangeRequestSortTarget.Mod) {
                        orderField = JsonConstants.Lsn;
                    } else if (queryInfo.sortTarget === RangeRequestSortTarget.Value) {
                        orderField = JsonConstants.Value;
                    } else if (queryInfo.sortTarget === RangeRequestSortTarget.Version) {
                        orderField = JsonConstants.Version;
                    }

                    if (orderField) {
                        return `ORDER BY c.${orderField} ${order}`;
                    }
                }

                return "";
            }

            static combineWhereClauses(whereClauses) {
                var whereclause = "WHERE ";
                if (whereClauses.length > 0) {
                    whereclause += whereClauses.join(" AND ");
                }
                return whereclause;
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        /// Helper API
        ////////////////////////////////////////////////////////////////////////////
        function generateUUID() {
            var d = new Date().getTime();
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,
                function(c) {
                    var r = (d + Math.random() * 16) % 16 | 0;
                    d = Math.floor(d / 16);
                    return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
                });
        }

        ////////////////////////////////////////////////////////////////////////////
        /// Async API for DocumentDB:
        /// The javascript client can call 
        /// await createDocumentAsync(kv)
        ////////////////////////////////////////////////////////////////////////////
        function createDocumentAsync(doc) {
            var collection = getContext().getCollection();
            var executor = (resolve, reject) => {
                var isAccepted = collection.createDocument(collection.getSelfLink(),
                    doc,
                    (error, resources, options) => {
                        if (error) {
                            reject(error);
                        } else {
                            resolve(resources);
                        }
                    });
                if (!isAccepted) {
                    reject(new Error(GrpcError.ErrAsyncNotAccepted.detail));
                }
            };
            return new Promise(executor);
        }

        function replaceDocumentAsync(doc, inoptions) {
            var collection = getContext().getCollection();
            var executor = (resolve, reject) => {
                var accepted = collection.replaceDocument(doc._self,
                    doc,
                    inoptions,
                    (error, resources, options) => {
                        if (error) {
                            reject(error);
                        } else {
                            resolve(resources);
                        }
                    });
                if (!accepted) {
                    reject(new Error(GrpcError.ErrAsyncNotAccepted.detail));
                }
            };
            return new Promise(executor);
        }

        function deleteDocumentAsync(selflink) {
            var collection = getContext().getCollection();
            var executor = (resolve, reject) => {
                var accepted = collection.deleteDocument(selflink,
                    (error, options) => {
                        if (error) {
                            reject(error);
                        } else {
                            resolve();
                        }
                    });
                if (!accepted) {
                    reject(new Error(GrpcError.ErrAsyncNotAccepted.detail));
                }
            };
            return new Promise(executor);
        }

        function queryDocumentsAsync(query, feedOptions, followContinuation) {
            var collection = getContext().getCollection();
            if (!feedOptions) {
                feedOptions = { enableScan: true };
            }
            var executor = (resolve, reject) => {
                var pages = [];
                var requestOptions = Object.assign({}, feedOptions);
                var queryNext = () => {
                    var accepted = collection.queryDocuments(collection.getSelfLink(),
                        query,
                        requestOptions,
                        (error, resources, responseOptions) => {
                            if (error) {
                                reject(error);
                            } else {
                                if (resources && resources.length) {
                                    pages.push(resources);
                                }

                                // follow continuation tokens if the full list of matching results are needed.
                                // this is susceptible to execution timeout in a stored procedure, but we need to do this to ensure
                                // correctness. This needs to be optimized or changed to a fundamentally different design.
                                if (followContinuation && responseOptions && responseOptions.continuation) {
                                    requestOptions.continuation = responseOptions.continuation;
                                    queryNext();
                                } else {
                                    var flattened = [].concat.apply([], pages);
                                    resolve(flattened);
                                }
                            }
                        });
                    if (!accepted) {
                        reject(new Error(GrpcError.ErrAsyncNotAccepted.detail));
                    }
                };

                queryNext();
            };
            return new Promise(executor);
        }

        class StringUtil {
            static HashCode(value) {
                var hash = 0, i, chr;
                if (value.length === 0)
                    return hash;
                for (i = 0; i < value.length; i++) {
                    chr = value.charCodeAt(i);
                    hash = ((hash << 5) - hash) + chr;
                    hash |= 0; // Convert to 32bit integer
                }
                return hash;
            }
        }

        // https://github.com/coreos/etcd/blob/master/etcdserver/api/v3rpc/rpctypes/error.go
        var GrpcStatusCode;
        (function(GrpcStatusCode) {
            GrpcStatusCode[GrpcStatusCode["OK"] = 0] = "OK";
            GrpcStatusCode[GrpcStatusCode["Cancelled"] = 1] = "Cancelled";
            GrpcStatusCode[GrpcStatusCode["Unknown"] = 2] = "Unknown";
            GrpcStatusCode[GrpcStatusCode["InvalidArgument"] = 3] = "InvalidArgument";
            GrpcStatusCode[GrpcStatusCode["DeadlineExceeded"] = 4] = "DeadlineExceeded";
            GrpcStatusCode[GrpcStatusCode["NotFound"] = 5] = "NotFound";
            GrpcStatusCode[GrpcStatusCode["AlreadyExists"] = 6] = "AlreadyExists";
            GrpcStatusCode[GrpcStatusCode["PermissionDenied"] = 7] = "PermissionDenied";
            GrpcStatusCode[GrpcStatusCode["ResourceExhausted"] = 8] = "ResourceExhausted";
            GrpcStatusCode[GrpcStatusCode["FailedPrecondition"] = 9] = "FailedPrecondition";
            GrpcStatusCode[GrpcStatusCode["Aborted"] = 10] = "Aborted";
            GrpcStatusCode[GrpcStatusCode["OutOfRange"] = 11] = "OutOfRange";
            GrpcStatusCode[GrpcStatusCode["Unimplemented"] = 12] = "Unimplemented";
            GrpcStatusCode[GrpcStatusCode["Internal"] = 13] = "Internal";
            GrpcStatusCode[GrpcStatusCode["Unavailable"] = 14] = "Unavailable";
            GrpcStatusCode[GrpcStatusCode["DataLoss"] = 15] = "DataLoss";
            GrpcStatusCode[GrpcStatusCode["Unauthenticated"] = 16] = "Unauthenticated";
        })(GrpcStatusCode || (GrpcStatusCode = {}));
        var GrpcError = {
            ErrGRPCEmptyKey: { code: GrpcStatusCode.InvalidArgument, detail: "etcdserver: key is not provided" },
            ErrGRPCKeyNotFound: { code: GrpcStatusCode.InvalidArgument, detail: "etcdserver: key not found" },
            ErrGRPCValueProvided: { code: GrpcStatusCode.InvalidArgument, detail: "etcdserver: value is provided" },
            ErrGRPCLeaseProvided: { code: GrpcStatusCode.InvalidArgument, detail: "etcdserver: lease is provided" },
            ErrGRPCTooManyOps: { code: GrpcStatusCode.InvalidArgument, detail: "etcdserver: too many operations in txn request" },
            ErrGRPCDuplicateKey: { code: GrpcStatusCode.InvalidArgument, detail: "etcdserver: duplicate key given in txn request" },
            ErrGRPCCompacted: { code: GrpcStatusCode.OutOfRange, detail: "etcdserver: mvcc: required revision has been compacted" },
            ErrGRPCFutureRev: { code: GrpcStatusCode.OutOfRange, detail: "etcdserver: mvcc: required revision is a future revision" },
            ErrGRPCNoSpace: { code: GrpcStatusCode.ResourceExhausted, detail: "etcdserver: mvcc: database space exceeded" },
            ErrGRPCLeaseNotFound: { code: GrpcStatusCode.NotFound, detail: "etcdserver: requested lease not found" },
            ErrGRPCLeaseExist: { code: GrpcStatusCode.FailedPrecondition, detail: "etcdserver: lease already exists" },
            ErrAsyncNotAccepted: { code: GrpcStatusCode.Aborted, detail: "The operation encountered a transient error. This code only occurs on write operations. It is safe to retry the operation" }
        };
    }
    setupSystemFunctions();
}
//---------------------------------------------------------------------------------------------------