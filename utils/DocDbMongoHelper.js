//---------------------------------------------------------------------------------------------------
//
// In this script, keep only scripts that modify/update the store. Don't add read-only and compute-only scripts.
//
//---------------------------------------------------------------------------------------------------
// Code should run in strict mode wherever possible.
"use strict";
//---------------------------------------------------------------------------------------------------
if (typeof __ != "undefined") {
    // Use 'let' inside a block so that everything defined here is not visibile to user script.
    let setupSystemFunctions = function () {
        if (typeof __.sys == "undefined") __.sys = new Object();
        setupSys.call(__.sys);
        //---------------------------------------------------------------------------------------------------
        function setupSys() {
            // Note: set enumerable to true, if you want the function to be enumerated as properties of the __ object.
            Object.defineProperty(this, "commonUpdate", { enumerable: false, configurable: false, writable: false, value: commonUpdate });
            Object.defineProperty(this, "commonUpdate_BsonSchema", { enumerable: false, configurable: false, writable: false, value: commonUpdate_BsonSchema });
            Object.defineProperty(this, "commonDelete", { enumerable: false, configurable: false, writable: false, value: commonDelete });
            Object.defineProperty(this, "commonDeleteWithQueryString", { enumerable: false, configurable: false, writable: false, value: commonDeleteWithQueryString });
            Object.defineProperty(this, "commonCount", { enumerable: false, configurable: false, writable: false, value: commonCount });
            Object.defineProperty(this, "commonBulkInsert", { enumerable: false, configurable: false, writable: false, value: commonBulkInsert });
            Object.defineProperty(this, "echo", { enumerable: false, configurable: false, writable: false, value: echo });
            Object.defineProperty(this, "bulkPatch", { enumerable: false, configurable: false, writable: false, value: bulkPatch });
            Object.defineProperty(this, "internalQueueAPI_getItem", { enumerable: false, configurable: false, writable: false, value: internalQueueAPI_getItem });
            Object.defineProperty(this, "getOrUpdateAgentConfig", { enumerable: false, configurable: false, writable: false, value: getOrUpdateAgentConfig });
            Object.defineProperty(this, "getStorageAccountKey", { enumerable: false, configurable: false, writable: false, value: getStorageAccountKey });
            Object.defineProperty(this, "internalQueueAPI_updateMessageVisibility", { enumerable: false, configurable: false, writable: false, value: internalQueueAPI_updateMessageVisibility });
        }
        //---------------------------------------------------------------------------------------------------
        var InternalErrors = {
            Retry: "RetrySystemStoredProcedure",
            Conflict: "ResourceWithSpecifiedIdOrNameAlreadyExists",
            PushOperatorRequiresTargetArray: "PushOperatorRequiresTargetArray",
        }
        function getError(errNum) {
            return errNum === ErrorCodes.Conflict ? InternalErrors.Conflict : InternalErrors.Retry;
        }
        //---------------------------------------------------------------------------------------------------
        const HttpStatusCode = {
            Created: 201,
            BadRequest: 400,
            NotFound: 404,
            Conflict: 409,
            NotAccepted: 429,
            InternalError: 500
        };

        const ChakraErrorCodes = {
            JSERR_CantAssignToReadOnly: 0x800A13B5 | 0
        };
        const errorMessages = {
            commonDml_invalidArgument: 'Invalid argument. %s.',
            commonDml_invalidUpdateOperationType: 'Invalid update operation type "%s".',
            invalidFunctionCall: 'The function "%s" requires at least %s argument(s) but was called with %s argument(s).',
            argumentMustBeArray: 'The "docs" parameter must be an array. Got "%s".',
            systemCollectionIdMustBeString: 'The systemCollectionId option value must be a string.',
            docsMustBeObjectForSystemCollectionId: 'Input documents must be object when systemCollectionId option is specified.',
            notValidDecimal128: '%s is not a valid Decimal128.',
            incompatDecimal128NonDecimal128: 'Operation not supported between Decimal128 and other types.',
            unequalLength: 'Unequal length in operation %s.',
            positionalUpdateOperatorDidNotFindMatch: 'The positional operator did not find the match needed from the query.',
        };

        const NumericalRanges = {
            maxOverflowSafe: 9223372036854775000,
            minOverflowSafe: -9223372036854775000,
            int32MaxValue: 2147483647,
            int32MinValue: -2147483648,
        };

        const getStorageAccountKeyFunctionName ="secretLiteralDB8780B2-89FF-49F0-B9DA-AFF274881E0B";
        //---------------------------------------------------------------------------------------------------
        // Given a numerical value, return its value within the safe-from-overflow limits
        function limitValueToOverflowSafeLimits(value) {
            if (value < NumericalRanges.minOverflowSafe) {
                return NumericalRanges.minOverflowSafe;
            }
            else if (value > NumericalRanges.maxOverflowSafe) {
                return NumericalRanges.maxOverflowSafe;
            }
            return value;
        }

        //---------------------------------------------------------------------------------------------------
        /**
        * System Function: commonUpdate
        * @param {QuerySpec} querySpec - query specifications including:
        *       @property {string} rootSymbol - specifies the query root along with any bindings associated with the query
                                              - Example: root JOIN elem1 IN root["results"] 
        *       @property {string} filterExpr
        *       @property {string} orderByExpr
        *       @property {int} limit
        *       @property {string} queryRoot - specifies the query root denoted by SqlQueryRoot in MongoParserBase
        *       @property {string} positionalUpdateExpr - specifies top level supquery to get indexes for positional update operator
        * @param {UpdateSpec} updateSpec - update specifications including:
        *       @property {IList<UpdateOperation>} operations
        *       @property {bool} multi
        *       @property {bool} upsert
        *       @property {object} upsertDocument
        * @param {string} fields - fields for projection
        * @param {SystemStoredProcedureOptions} updateOptions - system stored procedure properties including: 
        *       @property {string} continuation
        *       @property {string[]} unprocessedDocIds
        *       @property {bool} isFindAndModify
        *       @property {bool} returnModified
        *       @property {bool} remove
        *       @property {bool} isParseSystemCollection
        */
        function commonUpdate(querySpec, updateSpec, fields, updateOptions) {
            // --------------------------------------
            // Argument Validation
            // --------------------------------------
            if (arguments.length < 2)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.invalidFunctionCall, 'commonUpdate', 2, arguments.length));
            if (!querySpec)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec must be specified'));
            if (querySpec.filterExpr && (typeof (querySpec.filterExpr) != 'string'))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec filter expression must be a valid string'));
            if (((typeof querySpec.rootSymbol) != 'string') || (querySpec.rootSymbol === ''))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec root symbol must be specified'));
            if (((typeof querySpec.queryRoot) != 'string') || (querySpec.queryRoot === ''))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query root must be specified'));
            if (!updateSpec)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update spec must be specified'));
            if (updateSpec.operations === undefined)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update operations must be specified'));
            if (!Array.isArray(updateSpec.operations))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update operations must be an array'));
            if (!updateOptions)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update options spec must be specified'));
            if (updateOptions.unprocessedDocIds && !Array.isArray(updateOptions.unprocessedDocIds))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'updateOptions.unprocessedDocId must be an array'));

            // --------------------------------------
            // Scenarios:
            // - Update: can operate on multiple docs.
            // - FindAndModify: can operate on 1 docs only.
            // - Delete: operates on 1 doc (1st one), if the query results in more, the rest are ignored.

            var startTimeMillisecs = Date.now();

            // --------------------------------------
            // Main
            // --------------------------------------
            // Generate SQL query
            var queryString = sprintf('SELECT %s VALUE %s FROM %s', updateSpec.multi ? '' : 'TOP 1', querySpec.queryRoot, querySpec.rootSymbol);
            if (querySpec.filterExpr) {
                queryString += ' WHERE ' + querySpec.filterExpr;
            }

            if (querySpec.orderByExpr) {
                queryString += ' ORDER BY ' + querySpec.orderByExpr;
            }

            // When SqlParameters are provided, create a QuerySpec object, otherwise just pass the string.
            var queryObject;
            if (querySpec.sqlParameterCollection) {
                queryObject = {
                    query: queryString,
                    parameters: querySpec.sqlParameterCollection
                }
            } else {
                queryObject = queryString;
            }

            var updateOperations = updateSpec.operations;
            var upsertDocument = updateSpec.upsertDocument;
            var upsertExecuted = false;

            // response data for update
            var matchedCount = 0;
            var modifiedCount = 0;

            // response data for findAndModify
            var resultDocumentValue = null;     // Note: this is also used for delete.
            var updatedExisting = false;
            var remainingProcessPendingDocsCall = 100;  // Prevent call stack from infinite grow.

            if (updateOptions.unprocessedDocIds) {
                processPendingDocs(updateOptions.unprocessedDocIds, updateOptions.continuation);
            } else {
                loopNextQuery(updateOptions.continuation);
            }

            function getResponse(responseContinuation, unprocessedDocIds, error) {
                if (updateOptions.isFindAndModify === true) {

                    if (updateOptions.isParseSystemCollection === true && resultDocumentValue) {
                        resultDocumentValue = resultDocumentValue.value;
                    }

                    if (fields && resultDocumentValue) {
                        // if projection fields are explicitly passed in, then strip the result from the query to pass back only those
                        // fields that are expected. 
                        // Note: If a requested field is not part of the filtered document, skip it in the result
                        var projectedDocument = {};

                        var fieldInput = JSON.parse(fields);
                        if (Object.keys(fieldInput).length > 0) {
                            var queryDocument = resultDocumentValue;
                            //check if projection document only excludes fields
                            var onlyExcludesFields = true;
                            for (var property in fieldInput) {
                                if (fieldInput[property]) {
                                    onlyExcludesFields = false;
                                    break;
                                }
                            }

                            if (onlyExcludesFields) {
                                for (var property in queryDocument) {
                                    if (!fieldInput.hasOwnProperty(property)) {
                                        //if all fields in projection document are exclusion
                                        //then include all fields from document which are not found in projection document
                                        projectedDocument[property] = queryDocument[property];
                                    }
                                }
                            }
                            else {
                                for (var property in fieldInput) {
                                    if (fieldInput[property] && queryDocument.hasOwnProperty(property)) {
                                        //if projection document contains atleast one explicit inclusion
                                        //then include only fields listed for inclusion in projection document
                                        projectedDocument[property] = queryDocument[property];
                                    }
                                }
                            }
                            return { continuation: responseContinuation, updateResult: { updatedDocumentValue: projectedDocument, updatedExisting: updatedExisting, queryObject: queryObject } };
                        } else {
                            return { continuation: responseContinuation, updateResult: { updatedDocumentValue: resultDocumentValue, updatedExisting: updatedExisting, queryObject: queryObject } };
                        }

                    } else {
                        return { continuation: responseContinuation, updateResult: { updatedDocumentValue: resultDocumentValue, updatedExisting: updatedExisting, queryObject: queryObject } };
                    }
                } else {
                    if (upsertExecuted) {
                        return { continuation: responseContinuation, updateResult: { matchedCount: matchedCount, modifiedCount: modifiedCount, queryObject: queryObject, upsertedId: upsertDocument._id } };
                    } else {
                        var result = { continuation: responseContinuation, updateResult: { matchedCount: matchedCount, modifiedCount: modifiedCount, queryObject: queryObject } };
                        if (unprocessedDocIds) {
                            result.unprocessedDocIds = unprocessedDocIds;
                        }
                        if (error) {
                            result.updateResult.error = error;
                        }
                        return result;
                    }
                }
            }

            // Main query loop
            // Note: updateOptions.{maxMatchedCount, maxModifiedCount, pageSize} are used for testing.
            function loopNextQuery(continuation) {
                let pageSize = 1;
                let isAcceptedQuery = true;

                if (updateOptions.pageSize !== undefined) pageSize = updateOptions.pageSize;    // Testing-only.
                else {
                    const TimeLimitMillisecs = updateOptions.timeLimit > 0 ? updateOptions.timeLimit : 3000;
                    const ElapsedTime = Date.now() - startTimeMillisecs;
                    const RemainingTime = TimeLimitMillisecs - ElapsedTime;
                    if (RemainingTime <= 0) isAcceptedQuery = false;
                    else {
                        const UpdateTimePerDocMillisecs = modifiedCount > 0 ? ElapsedTime / modifiedCount : 8;
                        pageSize = (RemainingTime / UpdateTimePerDocMillisecs) | 0;
                        if (updateOptions.maxPageSize && updateOptions.maxPageSize > 0 && pageSize > updateOptions.maxPageSize) {
                            pageSize = updateOptions.maxPageSize
                        }
                        if (pageSize <= 3) pageSize = 1;
                    }
                }

                if (updateOptions.maxMatchedCount && matchedCount >= updateOptions.maxMatchedCount) { // Testing-only.
                    isAcceptedQuery = false;
                }

                isAcceptedQuery = isAcceptedQuery && __.queryDocuments(__.getSelfLink(), queryObject, { pageSize: pageSize, continuation: continuation }, queryCallback)
                if (!isAcceptedQuery) {
                    __.response.setBody(getResponse(continuation));
                    return;
                }
            }

            // Query callback
            function queryCallback(err, results, queryResponseOptions) {
                if (err) throw err;

                if (results.length === 0) {
                    if (queryResponseOptions.continuation) {
                        loopNextQuery(queryResponseOptions.continuation);
                        return;
                    } else if (updateSpec.upsert === true) {
                        createDocument();
                        upsertExecuted = true;
                        return;
                    }
                    else {
                        __.response.setBody(getResponse(undefined));
                        return;
                    }
                }

                if (updateOptions.remove) {
                    removeDocument(results[0]); // This command will always yield atmost a single result
                    return;
                }

                let resultIndex = 0;
                matchedCount += results.length;

                if (results.length > 0) {
                    processOneResult();
                }

                function processOneResult() {
                    if (!results || results.length == 0) throw new Error(ErrorCodes.InternalServerError, "Internal error. We shouldn't get empty results in processResult.");

                    var doc = results[resultIndex];
                    var docLink = doc._self;

                    updatedExisting = true;

                    // First, copy original document, in case we need to return unmodified.
                    if (isResultDocNeeded()) {
                        resultDocumentValue = deepCopy(doc);
                    }

                    // Apply each update operation to the retrieved document
                    var result = null;
                    var wasUpdated = false;
                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = false;

                        try {
                            result = handleUpdate(operation, doc, isCreate);
                            wasUpdated = wasUpdated || result.wasUpdated;
                            doc = result.doc;
                        } catch (err) {
                            if (updateSpec.multi && !updateOptions.isFindAndModify) {
                                __.response.setBody(getResponse(undefined, undefined, err.message));
                                return;
                            } else {
                                throw err;
                            }
                        }
                    }

                    let isAcceptedUpdate = true;
                    if (updateOptions.maxModifiedCount && modifiedCount >= updateOptions.maxModifiedCount) { // Testing-only.
                        isAcceptedUpdate = false;
                    }

                    if (wasUpdated === false) {
                        processNextUpdate();
                    }
                    else {
                        isAcceptedUpdate = isAcceptedUpdate && __.replaceDocument(doc._self, doc, { etag: doc._etag }, updateCallback);
                    }

                    if (!isAcceptedUpdate) {
                        var unprocessedDocIds = getDocIds(results, resultIndex);    // Current doc will be processed again.
                        __.response.setBody(getResponse(queryResponseOptions.continuation, unprocessedDocIds));
                        return;
                    }

                    if (isResultDocNeeded() && updateOptions.returnModified) {
                        resultDocumentValue = doc;
                    }
                }

                function updateCallback(err) {
                    if (err) {
                        throw new Error(err.number, getError(err.number));
                    }

                    modifiedCount++;
                    processNextUpdate();
                }

                function processNextUpdate() {
                    resultIndex++;

                    if (resultIndex < results.length) {
                        processOneResult();
                    } else if (queryResponseOptions.continuation) {
                        loopNextQuery(queryResponseOptions.continuation);
                    } else {
                        __.response.setBody(getResponse(undefined));
                        return;
                    }
                }
            } // queryCallback.

            // This should be called only in very rare case, as we dynamically adjust pageSize, but technically is still possible.
            function processPendingDocs(ids, continuation) {
                if (ids.length == 0) {
                    if (continuation) {
                        loopNextQuery(continuation);
                    } else {
                        __.response.setBody(getResponse());
                    }
                    return;
                }

                let id = ids.shift();
                let isAcceptedRead = __.readDocument(__.getAltLink() + '/docs/' + id, readCallback);
                if (!isAcceptedRead) {
                    ids.unshift(id);
                    __.response.setBody(getResponse(updateOptions.continuation, ids));
                }

                function readCallback(err, doc, options) {
                    if (err) {
                        if (err.number == HttpStatusCode.NotFound) {
                            // Ignore deleted doc.
                            if (remainingProcessPendingDocsCall-- > 0) processPendingDocs(ids, continuation);
                            else __.response.setBody(getResponse(updateOptions.continuation, ids));
                            return;
                        } else throw new Error(err.number, "RetrySystemStoredProcedure");
                    }

                    // Apply each update operation to the retrieved document
                    var wasUpdated = false;
                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = false;
                        var result = handleUpdate(operation, doc, isCreate);
                        wasUpdated = wasUpdated || result.wasUpdated;
                        doc = result.doc;
                    }

                    var isAcceptedUpdate = true;
                    if (wasUpdated === false) {
                        processPendingDocs(ids, continuation);
                    }
                    else {
                        isAcceptedUpdate = __.replaceDocument(doc._self, doc, { etag: doc._etag }, updateCallback);
                    }

                    if (!isAcceptedUpdate) {
                        ids.unshift(id);
                        __.response.setBody(getResponse(updateOptions.continuation, ids));
                    }
                }

                function updateCallback(err) {
                    if (err) {
                        throw new Error(err.number, getError(err.number));
                    }

                    ++modifiedCount;
                    processPendingDocs(ids, continuation);
                }
            }

            function createDocument() {
                if (upsertDocument) {

                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = true;

                        var result = handleUpdate(operation, upsertDocument, isCreate);
                        upsertDocument = result.doc;
                    }

                    var isAcceptedCreate = __.createDocument(__.getSelfLink(), upsertDocument, createCallback);

                    if (updateOptions.returnModified) {
                        resultDocumentValue = upsertDocument;
                    }

                    if (!isAcceptedCreate) {
                        __.response.setBody(getResponse(updateOptions.continuation));
                        return;
                    }
                } else {
                    throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, "Upsert document should have a value."));
                }
            }

            function createCallback(err) {
                if (err) {
                    if (err.number === ErrorCodes.Conflict) {
                        __.response.setBody(getResponse(undefined));
                        throw new Error(ErrorCodes.BadRequest, "ResourceWithSpecifiedIdOrNameAlreadyExists");
                    }

                    throw err;
                }

                __.response.setBody(getResponse(undefined));
                return;
            }

            function handleUpdate(operation, doc, isCreate) {
                var wasUpdated = true;
                var old_id = JSON.stringify(doc._id);

                try {
                    switch (operation.type) {
                        // Field Operations
                        case 'Inc':
                            fieldInc(doc, operation.field, operation.value);
                            break;
                        case 'Mul':
                            fieldMul(doc, operation.field, operation.value);
                            break;
                        case 'Min':
                            fieldMin(doc, operation.field, operation.value);
                            break;
                        case 'Max':
                            fieldMax(doc, operation.field, operation.value);
                            break;
                        case 'Rename':
                            fieldRename(doc, operation.field, operation.value);
                            break;
                        case 'Set':
                            wasUpdated = fieldSet(doc, operation.field, operation.value);
                            break;
                        case 'Unset':
                            fieldUnset(doc, operation.field);
                            break;
                        case 'CurrentDate':
                            fieldCurrentDate(doc, operation.field, operation.value);
                            break;
                        case 'Bit':
                            fieldBit(doc, operation.field, operation.value, operation.operator);
                            break;

                        // Array Operations
                        case 'AddToSet':
                            arrayAddToSet(doc, operation.field, operation.value);
                            break;
                        case 'Pop':
                            arrayPop(doc, operation.field, operation.value);
                            break;
                        case 'PullAll':
                            arrayPullAll(doc, operation.field, operation.value);
                            break;
                        case 'Pull':
                            arrayPull(doc, operation.field, operation.value);
                            break;
                        case 'Push':
                            arrayPush(doc, operation.field, operation.value, operation.slice, operation.position);
                            break;

                        // Full replace
                        case 'Replace':
                            doc = fullReplace(doc, operation.value);
                            break;

                        case 'SetOnInsert':
                            if (updateSpec.upsert === true && isCreate) {
                                fieldSet(doc, operation.field, operation.value);
                            }
                            break;

                        case 'SetOnCondition':
                            fieldSetOnCondition(doc, operation.field, updateSpec.upsert === true && isCreate, operation.value, operation.operator, operation.conditionField, operation.rhsValue)
                            break;

                        default:
                            throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidUpdateOperationType, operation.type));
                    }
                } catch (err) {
                    if (err.number == ChakraErrorCodes.JSERR_CantAssignToReadOnly) {
                        //  This can only be hit due to strict mode, as we don't explicitly use read-only properties for DML.
                        //  Convert to graceful error, to correspond to original behavior.
                        // Scenario (incorrect usage): insert({id:1, n:"foo"}); update({ id: 1 }, { $set: { 'n.x': 2 } });
                        // This error is handled in Gateway and converted to a write error.
                        throw new Error(ErrorCodes.BadRequest, "CannotUsePartOfPropertyToTraverseElement");
                    }
                    throw err;
                }

                if (old_id !== JSON.stringify(doc._id) && !isCreate) {
                    throw new Error(ErrorCodes.BadRequest, "CannotUpdateImmutableField");
                }
                return { wasUpdated: wasUpdated, doc: doc };
            }

            function removeDocument(doc) {
                var isAcceptedRemove = __.deleteDocument(doc._self, {}, removeCallback);
                resultDocumentValue = doc;
            }

            function removeCallback(err) {
                if (err) {
                    throw err;
                }

                __.response.setBody(getResponse(undefined));
                return;
            }

            function getDocIds(docs, startIndex) {
                if (!docs || startIndex >= docs.length) return undefined;

                var results = [];
                for (var i = startIndex; i < docs.length; ++i) {
                    results.push(docs[i].id);
                }

                return results;
            }

            function isResultDocNeeded() {
                return updateOptions.isFindAndModify || updateOptions.remove;
            }

            function deepCopy(obj) {
                var copy;

                // Handle the 3 simple types, and null or undefined
                if (null == obj || "object" != typeof obj) return obj;

                // Handle Array
                if (obj instanceof Array) {
                    copy = [];
                    for (var i = 0, len = obj.length; i < len; i++) {
                        copy[i] = deepCopy(obj[i]);
                    }
                    return copy;
                }

                // Handle Object
                if (obj instanceof Object) {
                    copy = {};
                    for (var attr in obj) {
                        if (obj.hasOwnProperty(attr)) copy[attr] = deepCopy(obj[attr]);
                    }
                    return copy;
                }
            }

            // --------------------------------------
            // Field Operations
            // --------------------------------------

            /****
            Note on native MongoDB behavior, using Mongoshell.

            > t.insert({a:1,b:9223372036854775296})  // b is double, gets rounded to a value greater than Int64.Max_value
            "b" : 9223372036854776000  // <-- trying to cast this to Int64, MongoDB.Bson throws an overflow exception when reading the document in the Gateway.

            On the other hand, 9223372036854775295 (double) is stored as 9223372036854775000 (0x7FFFFFFFFFFFFCD8).

            > t.insert({a:1,b:NumberLong(9223372036854775296)}) // b is defined as NumberLong(Int64), value is less than Int64.MaxValue
            2018-03-20T15:20:08.482-0700 E QUERY    [thread1] Error: number passed to NumberLong must be representable as an int64_t :
            @(shell):1:17

            Note on DocDB behavior:
            In old schema, reading from the backend any value greater than Int64.Max_Value will cause MongoDB.Bson.Io.BsonReader 
            to throw an exception ("Value was either too large or too small for an Int64.") during the transformation of the document from Json to Bson.
            However, if the number increases 3 orders of magnitude (1000x) beyond Int64.Max_Value, it forces an exponential representation and the bug is circumvented.
            
            For new (Bson) schema, doubles are allowed to increase freely with loss of precision, Int64s are limited to overflow-safe limits (as old schema), and Int32s are overflown into Int64s.
            ****/

            // Operation: $inc
            //   Increments the value of the field by the specified amount.
            function fieldInc(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    if ((typeof oldValue) == "number")
                        return oldValue + value;
                    return oldValue;
                });
            }

            // Operation: $mul
            //   Multiplies the value of the field by the specified amount.
            function fieldMul(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return 0;
                    if ((typeof oldValue) == "number")
                        return oldValue * value;
                    return oldValue;
                });
            }

            // Operation: $min
            //   Only updates the field if the specified value is less than the existing field value.
            function fieldMin(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    return compare(value, oldValue) < 0 ? value : oldValue;
                });
            }

            // Operation: $max
            //   Only updates the field if the specified value is greater than the existing field value.
            function fieldMax(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    return compare(value, oldValue) > 0 ? value : oldValue;
                });
            }

            // Operation: $rename
            //   Renames a field.
            function fieldRename(doc, field, newField) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires((typeof newField+ == 'string')
                // Requires(newField+ != '')

                var value = fieldUnset(doc, field);
                if (value !== undefined) {
                    fieldSet(doc, newField, value);
                }

                return value;
            }

            // Operation: $set
            //    Sets the value of a field in a document.
            function fieldSet(doc, field, value, applyOp) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)
                // Requires((applyOp === undefined) || (typeof(applyOp) == 'function'))

                var navResult = navigateTo(doc, field, true);

                doc = navResult[0];
                field = navResult[1];

                if (value === doc[field] && !applyOp) {
                    return false;
                }
                else {
                    doc[field] = applyOp ? applyOp(doc[field]) : value;
                    return true;
                }
            }

            // Operation: $unset
            //   Removes the specified field from a document.
            function fieldUnset(doc, field) {
                // Requires(doc != undefined)
                // Requires(field != undefined)

                var segments = field.split('.');
                var value;

                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    var seg = segments[i];

                    // if this is the last segment then we delete the field
                    if (i == (segments.length - 1)) {
                        value = doc[seg];
                        delete doc[seg];
                    } else {
                        // Advance to the next segment
                        doc = doc[seg];
                    }
                }

                return value;
            }

            // Operation: $currentDate
            //   Sets the value of a field to current date, either as a Date or a Timestamp.
            function fieldCurrentDate(doc, field, type) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires((type == 'date') || (type == 'timestamp'))

                var value = type === 'date' ?
                    { $date: new Date().toISOString() } :
                    { $timestamp: { t: Math.floor(new Date().getTime() / 1000), i: 1 } };

                fieldSet(doc, field, value);
            }

            // Operation: $bit
            //   Performs bitwise AND, OR, and XOR updates of integer values.
            function fieldBit(doc, field, value, op) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')
                // Requires((op == 'And') || (op == 'Or') || (op == 'Xor'))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if (doc && ((typeof doc[field]) == 'number')) {
                    switch (op) {
                        case 'And':
                            doc[field] &= value;
                            break;
                        case 'Or':
                            doc[field] |= value;
                            break;
                        case 'Xor':
                            doc[field] ^= value;
                            break;
                    }
                }
            }

            // --------------------------------------
            // Array Operations
            // --------------------------------------

            // Operation: $addToSet
            //  Adds elements to an array only if they do not already exist in the set.
            function arrayAddToSet(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = new Array();
                } else {
                    if (!Array.isArray(doc[field]))
                        throw new Error(ErrorCodes.BadRequest, 'AddToSet operation requires a target array field.');
                }

                for (var i = 0; i < values.length; i++) {
                    if (!arrayContains(doc[field], values[i])) {
                        doc[field].push(values[i]);
                    }
                }
            }

            // Operation: $pop
            //  Removes the first or last item of an array.
            function arrayPop(doc, field, firstLast) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(firstLast == -1 OR firstLast == 1)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    if (firstLast == -1) {
                        doc[field].shift();
                    } else if (firstLast == 1) {
                        doc[field].pop();
                    }
                }
            }

            // Operation: $pullAll
            //  Removes all matching values from an array.
            function arrayPullAll(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    var array = doc[field];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {
                        if (!arrayContains(values, array[i])) {
                            result.push(array[i]);
                        }
                    }

                    doc[field] = result;
                }
            }

            // Operation: $pull
            //  Removes all array elements that match a specified query.
            function arrayPull(doc, field, value) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(filter != undefined)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    var array = doc[field];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {
                        if (!arrayPullFilter(array[i], value)) {
                            result.push(array[i]);
                        }
                    }

                    doc[field] = result;
                }
            }

            // Operation: $push
            //  Adds an item to an array.
            function arrayPush(doc, field, values, sliceModifier, positionModifier) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = values;
                }
                else {
                    if (!Array.isArray(doc[field]))
                        throw new Error(ErrorCodes.BadRequest, InternalErrors.PushOperatorRequiresTargetArray);

                    if (positionModifier !== undefined) {
                        for (var i = 0; i < values.length; i++) {
                            doc[field].splice(positionModifier, 0, values[i]);
                            positionModifier++;
                        }
                    } else {
                        for (var i = 0; i < values.length; i++) {
                            doc[field].push(values[i]);
                        }
                    }
                }

                if (sliceModifier !== undefined) {
                    if (sliceModifier < 0) {
                        doc[field] = doc[field].slice(sliceModifier);
                    } else {
                        doc[field] = doc[field].slice(0, sliceModifier);
                    }
                }
            }

            function fullReplace(doc, value) {
                var temp = doc;
                if (updateOptions.isParseSystemCollection === true) {
                    if (value.hasOwnProperty("_id") && (JSON.stringify(doc.value._id) !== JSON.stringify(value._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc.value = value;
                    doc.value.id = temp.value.id;
                    doc.value._id = temp.value._id;
                }
                else {
                    if (value.hasOwnProperty("_id") && (JSON.stringify(doc._id) !== JSON.stringify(value._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc = value;
                    doc._id = temp._id;
                }

                doc.id = temp.id;
                doc._self = temp._self;
                doc._etag = temp._etag;

                return doc;
            }

            // Operation: $setOnCondition (used by cassandra)
            //  Sets a value only when the specified condition is met
            function fieldSetOnCondition(doc, path, upsert, newValue, operator, conditionField, rhsValue) {
                // Requires(doc != undefined)
                // Requires(path != undefined)
                // Requires(upsert != undefined)
                // Requires(newValue != undefined)
                // Requires(operator != undefined)
                // Requires(conditionField != undefined)
                // Requires(rhsValue != undefined)

                var navResult = navigateTo(doc, path, true);
                doc = navResult[0];
                var field = navResult[1];

                navResult = navigateTo(doc, conditionField, false);
                var condDoc = navResult[0];
                var condField = navResult[1];

                // if conditionField is not present,
                if (condDoc[condField] === undefined) {
                    // if the fieldToUpdate is not present, then upsert if specified
                    if (doc[field] === undefined && upsert) {
                        doc[field] = newValue;
                    }

                    //if the fieldToUpdate is present, then no-op (does not meet upsert semantics)
                }
                // if conditionField is present
                else {
                    // check the condition
                    if (checkCondition(condDoc[condField], operator, rhsValue) === 1) {
                        doc[field] = newValue;
                    }
                }
            }

            // --------------------------------------
            // Common Utility Functions
            // --------------------------------------
            const OperatorType = {
                None: 0,
                EQ: 1,
                LT: 2,
                LTE: 3,
                GT: 4,
                GTE: 5,
                NE: 6,
            };

            function navigateTo(doc, path, upsert) {
                // Requires(doc !== undefined)
                // Requires(path !== undefined)

                var segments = path.split('.');

                var seg;
                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    seg = segments[i];

                    // We stop at the last path segment and return its value
                    if (i == (segments.length - 1)) break;

                    // If upsert is set and the segment does not exist then create it
                    if (upsert && (doc[seg] === undefined)) {
                        doc[seg] = {};
                    }

                    // Advance to the next segment
                    doc = doc[seg];
                }

                return [doc, seg];
            }

            function arrayContains(array, item) {
                // Requires(Array.isArray(array))
                // Requires(item !=== undefined)

                if ((typeof item) == 'object') {
                    for (var i = 0; i < array.length; i++) {
                        if (compare(array[i], item) == 0)
                            return true;
                    }

                    return false;
                }

                return array.indexOf(item) >= 0;
            }

            function objectsEquivalent(left, right, depth) {
                if (Object.getPrototypeOf(left) !== Object.getPrototypeOf(right)) return false;

                var leftPropertyNames = Object.getOwnPropertyNames(left);
                var rightPropertyNames = Object.getOwnPropertyNames(right);

                if (leftPropertyNames.length != rightPropertyNames.length) {
                    return false;
                }

                for (var i = 0; i < leftPropertyNames.length; i++) {
                    var leftProp = leftPropertyNames[i];
                    var rightProp = rightPropertyNames[i];

                    // Mongo behavior: {a: 1, b: 2} != {b: 2, a: 1}
                    if (leftProp !== rightProp) {
                        return false;
                    }

                    if (typeof (left[leftProp]) == 'object') {
                        if (compare(left[leftProp], right[leftProp], depth + 1) != 0) {
                            return false;
                        }
                    } else {
                        if (left[leftProp] !== right[leftProp]) {
                            return false;
                        }
                    }
                }
                return true;
            }

            function arraysEquivalent(left, right, depth) {
                if (left === right) return true;
                if (left === null || right === null) return false;
                if (left.length != right.length) return false;

                if (Object.getOwnPropertyNames(left).length > left.length + 1 || Object.getOwnPropertyNames(right).length > right.length + 1) {
                    return objectsEquivalent(left, right, depth);
                }

                for (var i = 0; i < left.length; i++) {
                    if (compare(left[i], right[i], depth + 1) != 0) {
                        return false;
                    }
                }
                return true;
            }

            function compare(value1, value2, depth) {
                // Requires(value1 !== undefined)
                // Requires(value2 !== undefined)

                // To prevent infinite object property reference loop
                if (depth === undefined) depth = 1;
                if (depth > 1000) return false;

                var t1 = getTypeOrder(value1);
                var t2 = getTypeOrder(value2);

                if (t1 === t2) {
                    if (value1 == value2) return 0;

                    if (Array.isArray(value1)) {
                        if (arraysEquivalent(value1, value2, depth)) return 0;
                    }
                    else if (typeof value1 == 'object') {
                        if (objectsEquivalent(value1, value2, depth)) return 0;
                    } else {
                        return (value1 < value2) ? -1 : 1;
                    }

                    return (value1 < value2) ? -1 : 1;
                }

                return t1 < t2 ? -1 : 1;
            }

            const ValueMarker = "$v";
            function checkCondition(lhs, operator, rhs) {
                // Requires(lhs != undefined)
                // Requires(operator != undefined)
                // Requires(rhs != undefined)

                //BUGBUG: Cassandra should use commonUpdate_BsonSchema instead of commonUpdate sproc
                lhs = lhs[ValueMarker];
                rhs = rhs[ValueMarker];

                var result = 0;
                switch (operator) {
                    case OperatorType.EQ:
                        if (lhs === rhs) {
                            result = 1;
                        }
                        break;
                    case OperatorType.LT:
                        if (lhs < rhs) {
                            result = 1;
                        }
                        break;
                    case OperatorType.LTE:
                        if (lhs <= rhs) {
                            result = 1;
                        }
                        break;
                    case OperatorType.GT:
                        if (lhs > rhs) {
                            result = 1;
                        }
                        break;
                    case OperatorType.GTE:
                        if (lhs >= rhs) {
                            result = 1;
                        }
                        break;
                    case OperatorType.NE:
                        if (lhs != rhs) {
                            result = 1;
                        }
                        break;
                    default:
                        throw new Error(operator);
                }

                return result;
            }

            // If the specified <value> to remove is an array, $pull removes only the elements in the array that match the
            // specified <value> exactly, including order. If the specified <value> to remove is a document, $pull removes only
            // the elements in the array that have/[contain] the exact same fields and values. The ordering of the fields can differ.
            // Note: Mongo ignores the pull operator when the item to be removed (pullItem) is [] or {}.
            let arrayPullFilter = function (arrayItem, pullItem) {
                if (typeof pullItem == 'object' && pullItem !== null) {
                    if (typeof arrayItem == 'object' && arrayItem !== null) {
                        if (Array.isArray(arrayItem)) { // Array [] case
                            if (Array.isArray(pullItem) && (pullItem.length === arrayItem.length)) {
                                for (var i = 0; i < arrayItem.length; i++) {
                                    if (typeof pullItem[i] == 'object') {
                                        if (!arrayPullFilter(arrayItem[i], pullItem[i])) {
                                            return false;
                                        }
                                    } else {
                                        if (pullItem[i] !== arrayItem[i]) {
                                            return false;
                                        }
                                    }
                                }
                                return true;
                            } else {
                                return false;
                            }
                            return true;
                        } else { // Object {} case
                            if (Array.isArray(pullItem)) return false;
                            if (Object.keys(pullItem).length == 0) return true;

                            for (var p in pullItem) {
                                if (typeof pullItem[p] == 'object' && arrayItem.hasOwnProperty(p)) {
                                    if (!arrayPullFilter(arrayItem[p], pullItem[p])) {
                                        return false;
                                    }
                                } else {
                                    if (!arrayItem.hasOwnProperty(p) || !(arrayItem[p] === pullItem[p])) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        }
                    } else {
                        return false;
                    }
                }
                else {
                    return arrayItem === pullItem;
                }
            }

            function getTypeOrder(value) {
                // Requires(value !== undefined)

                // Here is the type ordering 
                // 1.MinKey (internal type)
                // 2.Null
                // 3.Numbers (ints, longs, doubles)
                // 4.Symbol, String
                // 5.Object
                // 6.Array
                // 7.BinData
                // 8.ObjectId
                // 9.Boolean
                // 10.Date, Timestamp
                // 11.Regular Expression
                // 12.MaxKey (internal type)

                switch (typeof value) {
                    case "number":
                        return 3;
                    case "string":
                    case "symbol":
                        return 4;
                    case "boolean":
                        return 9;
                    case "object":
                        if (value === null) return 2;
                        if (Array.isArray(value)) return 6;
                        return 5;
                    default:
                        return 12;
                }
            }
        }

        function commonUpdate_BsonSchema(querySpec, updateSpec, fields, updateOptions) {
            // --------------------------------------
            // Argument Validation
            // --------------------------------------
            if (arguments.length < 2)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.invalidFunctionCall, 'commonUpdate', 2, arguments.length));
            if (!querySpec)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec must be specified'));
            if (querySpec.filterExpr && (typeof (querySpec.filterExpr) != 'string'))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec filter expression must be a valid string'));
            if (((typeof querySpec.rootSymbol) != 'string') || (querySpec.rootSymbol === ''))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec root symbol must be specified'));
            if (((typeof querySpec.queryRoot) != 'string') || (querySpec.queryRoot === ''))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query root must be specified'));
            if (!updateSpec)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update spec must be specified'));
            if (updateSpec.operations === undefined)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update operations must be specified'));
            if (!Array.isArray(updateSpec.operations))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update operations must be an array'));
            if (!updateOptions)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Update options spec must be specified'));
            if (updateOptions.unprocessedDocIds && !Array.isArray(updateOptions.unprocessedDocIds))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'updateOptions.unprocessedDocId must be an array'));

            // --------------------------------------
            // Scenarios:
            // - Update: can operate on multiple docs.
            // - FindAndModify: can operate on 1 docs only.
            // - Delete: operates on 1 doc (1st one), if the query results in more, the rest are ignored.

            var startTimeMillisecs = Date.now();

            // --------------------------------------
            // Main
            // --------------------------------------
            // Generate SQL query
            if (querySpec.positionalUpdateExpr) {
                var queryString = `SELECT ${updateSpec.multi ? '' : 'TOP 1'} ${querySpec.queryRoot}, ${querySpec.positionalUpdateExpr} FROM ${querySpec.rootSymbol}`;
            } else {
                var queryString = `SELECT ${updateSpec.multi ? '' : 'TOP 1'} VALUE ${querySpec.queryRoot} FROM ${querySpec.rootSymbol}`;
            }

            if (querySpec.filterExpr) {
                queryString += ' WHERE ' + querySpec.filterExpr;
            }

            if (querySpec.orderByExpr) {
                queryString += ' ORDER BY ' + querySpec.orderByExpr;
            }

            // When SqlParameters are provided, create a QuerySpec object, otherwise just pass the string.
            var queryObject;
            if (querySpec.sqlParameterCollection) {
                queryObject = {
                    query: queryString,
                    parameters: querySpec.sqlParameterCollection
                }
            } else {
                queryObject = queryString;
            }

            var updateOperations = updateSpec.operations;
            var upsertDocument = updateSpec.upsertDocument;
            var upsertExecuted = false;

            // response data for update
            var matchedCount = 0;
            var modifiedCount = 0;

            // response data for findAndModify
            var resultDocumentValue = null;     // Note: this is also used for delete.
            var updatedExisting = false;
            var remainingProcessPendingDocsCall = 100;  // Prevent call stack from infinite grow.

            if (updateOptions.unprocessedDocIds) {
                processPendingDocs(updateOptions.unprocessedDocIds, updateOptions.continuation);
            } else {
                loopNextQuery(updateOptions.continuation);
            }

            function getResponse(responseContinuation, unprocessedDocIds, error) {
                if (updateOptions.isFindAndModify === true) {

                    if (updateOptions.isParseSystemCollection === true && resultDocumentValue) {
                        resultDocumentValue = resultDocumentValue.value;
                    }

                    if (fields && resultDocumentValue) {
                        // if projection fields are explicitly passed in, then strip the result from the query to pass back only those
                        // fields that are expected. 
                        // Note: If a requested field is not part of the filtered document, skip it in the result
                        var projectedDocument = {};

                        var fieldInput = JSON.parse(fields);
                        if (Object.keys(fieldInput).length > 0) {
                            var queryDocument = resultDocumentValue[ValueMarker];
                            //check if projection document only excludes fields
                            var onlyExcludesFields = true;
                            for (var property in fieldInput) {
                                if (fieldInput[property]) {
                                    onlyExcludesFields = false;
                                    break;
                                }
                            }

                            if (onlyExcludesFields) {
                                for (var property in queryDocument) {
                                    if (!fieldInput.hasOwnProperty(property)) {
                                        //if all fields in projection document are exclusion
                                        //then include all fields from document which are not found in projection document
                                        projectedDocument[property] = queryDocument[property];
                                    }
                                }
                            }
                            else {
                                for (var property in fieldInput) {
                                    if (fieldInput[property] && queryDocument.hasOwnProperty(property)) {
                                        //if projection document contains atleast one explicit inclusion
                                        //then include only fields listed for inclusion in projection document
                                        projectedDocument[property] = queryDocument[property];
                                    }
                                }
                            }

                            resultDocumentValue = {};
                            resultDocumentValue[TypeMarker] = BsonType.Document;
                            resultDocumentValue[ValueMarker] = projectedDocument;
                            return { continuation: responseContinuation, updateResult: { updatedDocumentValue: resultDocumentValue, updatedExisting: updatedExisting, queryObject: queryObject } };
                        } else {
                            return { continuation: responseContinuation, updateResult: { updatedDocumentValue: resultDocumentValue, updatedExisting: updatedExisting, queryObject: queryObject } };
                        }

                    } else {
                        return { continuation: responseContinuation, updateResult: { updatedDocumentValue: resultDocumentValue, updatedExisting: updatedExisting, queryObject: queryObject } };
                    }
                } else {
                    if (upsertExecuted) {
                        var upsertedId;
                        if (updateOptions.isParseSystemCollection === true) {
                            upsertedId = upsertDocument.value[ValueMarker]._id;
                        }
                        else {
                            upsertedId = upsertDocument[ValueMarker]._id;
                        }

                        return { continuation: responseContinuation, updateResult: { matchedCount: matchedCount, modifiedCount: modifiedCount, queryObject: queryObject, upsertedId: upsertedId } };
                    } else {
                        var result = { continuation: responseContinuation, updateResult: { matchedCount: matchedCount, modifiedCount: modifiedCount, queryObject: queryObject } };
                        if (unprocessedDocIds) {
                            result.unprocessedDocIds = unprocessedDocIds;
                        }
                        if (error) {
                            result.updateResult.error = error;
                        }
                        return result;
                    }
                }
            }

            // Main query loop
            // Note: updateOptions.{maxMatchedCount, maxModifiedCount, pageSize} are used for testing.
            function loopNextQuery(continuation) {
                let pageSize = 1;
                let isAcceptedQuery = true;

                if (updateOptions.pageSize !== undefined) pageSize = updateOptions.pageSize;    // Testing-only.
                else {
                    const TimeLimitMillisecs = updateOptions.timeLimit > 0 ? updateOptions.timeLimit : 3000;
                    const ElapsedTime = Date.now() - startTimeMillisecs;
                    const RemainingTime = TimeLimitMillisecs - ElapsedTime;
                    if (RemainingTime <= 0) isAcceptedQuery = false;
                    else {
                        const UpdateTimePerDocMillisecs = modifiedCount > 0 ? ElapsedTime / modifiedCount : 8;
                        pageSize = (RemainingTime / UpdateTimePerDocMillisecs) | 0;
                        if (updateOptions.maxPageSize && updateOptions.maxPageSize > 0 && pageSize > updateOptions.maxPageSize) {
                            pageSize = updateOptions.maxPageSize
                        }
                        if (pageSize <= 3) pageSize = 1;
                    }
                }

                if (updateOptions.maxMatchedCount && matchedCount >= updateOptions.maxMatchedCount) { // Testing-only.
                    isAcceptedQuery = false;
                }

                isAcceptedQuery = isAcceptedQuery && __.queryDocuments(__.getSelfLink(), queryObject, { pageSize: pageSize, continuation: continuation }, queryCallback)
                if (!isAcceptedQuery) {
                    __.response.setBody(getResponse(continuation));
                    return;
                }
            }

            // Query callback
            function queryCallback(err, results, queryResponseOptions) {
                if (err) throw err;

                if (results.length === 0) {
                    if (queryResponseOptions.continuation) {
                        loopNextQuery(queryResponseOptions.continuation);
                        return;
                    } else if (updateSpec.upsert === true) {
                        createDocument();
                        upsertExecuted = true;
                        return;
                    }
                    else {
                        __.response.setBody(getResponse(undefined));
                        return;
                    }
                }

                if (updateOptions.remove) {
                    if (querySpec.positionalUpdateExpr) {
                        removeDocument(results[0][querySpec.queryRoot]); // This command will always yield atmost a single result
                    } else {
                        removeDocument(results[0]); // This command will always yield atmost a single result
                    }

                    return;
                }

                let resultIndex = 0;
                matchedCount += results.length;

                if (results.length > 0) {
                    processOneResult();
                }

                function processOneResult() {
                    if (!results || results.length == 0) throw new Error(ErrorCodes.InternalServerError, "Internal error. We shouldn't get empty results in processResult.");

                    if (querySpec.positionalUpdateExpr) {
                        var doc = results[resultIndex][querySpec.queryRoot];
                    } else {
                        var doc = results[resultIndex];
                    }
                    var docLink = doc._self;

                    updatedExisting = true;

                    // First, copy original document, in case we need to return unmodified.
                    if (isResultDocNeeded()) {
                        resultDocumentValue = deepCopy(doc);
                    }

                    // Apply each update operation to the retrieved document
                    var result = null;
                    var wasUpdated = false;
                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = false;

                        if (querySpec.positionalUpdateExpr) {
                            // check if operation is positional update operator, get first index of match, and replace "$" with index
                            const innerPosUpdateOpSymbol = ".$v.$.", endPosUpdateOpSymbol = ".$v.$", posUpOpName = "positionalUpdateOperator";
                            try {
                                let positionalUpdateIndex = -1;
                                for (var projIndex = 0; projIndex < results[resultIndex][posUpOpName].length; projIndex++) {
                                    if (results[resultIndex][posUpOpName][projIndex].includes(true)) {
                                        positionalUpdateIndex = results[resultIndex][posUpOpName][projIndex].indexOf(true);
                                        break;
                                    }
                                }

                                if (operation.field.includes(innerPosUpdateOpSymbol)) {
                                    operation.field = operation.field.replace(innerPosUpdateOpSymbol, ".$v." + positionalUpdateIndex + ".");
                                } else if (operation.field.endsWith(endPosUpdateOpSymbol)) {
                                    operation.field = operation.field.replace(endPosUpdateOpSymbol, ".$v." + positionalUpdateIndex);
                                }
                            } catch (err) {
                                // this should only occur when results do not contain the array, this should not happen
                                throw new Error(ErrorCodes.BadRequest, errorMessages.positionalUpdateOperatorDidNotFindMatch);
                            }
                        }

                        try {
                            result = handleUpdate(operation, doc, isCreate);
                            wasUpdated = wasUpdated || result.wasUpdated;
                            doc = result.doc;
                        } catch (err) {
                            if (updateSpec.multi && !updateOptions.isFindAndModify) {
                                __.response.setBody(getResponse(undefined, undefined, err.message));
                                return;
                            } else {
                                throw err;
                            }
                        }
                    }

                    let isAcceptedUpdate = true;
                    if (updateOptions.maxModifiedCount && modifiedCount >= updateOptions.maxModifiedCount) { // Testing-only.
                        isAcceptedUpdate = false;
                    }

                    if (wasUpdated === false) {
                        processNextUpdate();
                    }
                    else {
                        isAcceptedUpdate = isAcceptedUpdate && __.replaceDocument(doc._self, doc, { etag: doc._etag }, updateCallback);
                    }

                    if (!isAcceptedUpdate) {
                        var unprocessedDocIds = getDocIds(results, resultIndex);    // Current doc will be processed again.
                        __.response.setBody(getResponse(queryResponseOptions.continuation, unprocessedDocIds));
                        return;
                    }

                    if (isResultDocNeeded() && updateOptions.returnModified) {
                        resultDocumentValue = doc;
                    }
                }

                function processNextUpdate() {
                    resultIndex++;

                    if (resultIndex < results.length) {
                        processOneResult();
                    } else if (queryResponseOptions.continuation) {
                        loopNextQuery(queryResponseOptions.continuation);
                    } else {
                        __.response.setBody(getResponse(undefined));
                        return;
                    }
                }

                function updateCallback(err) {
                    if (err) {
                        throw new Error(err.number, getError(err.number));
                    }

                    modifiedCount++;
                    processNextUpdate();
                }
            } // queryCallback.

            // This should be called only in very rare case, as we dynamically adjust pageSize, but technically is still possible.
            function processPendingDocs(ids, continuation) {
                if (ids.length == 0) {
                    if (continuation) {
                        loopNextQuery(continuation);
                    } else {
                        __.response.setBody(getResponse());
                    }
                    return;
                }

                let id = ids.shift();
                let isAcceptedRead = __.readDocument(__.getAltLink() + '/docs/' + id, readCallback);
                if (!isAcceptedRead) {
                    ids.unshift(id);
                    __.response.setBody(getResponse(updateOptions.continuation, ids));
                }

                function readCallback(err, doc, options) {
                    if (err) {
                        if (err.number == HttpStatusCode.NotFound) {
                            // Ignore deleted doc.
                            if (remainingProcessPendingDocsCall-- > 0) processPendingDocs(ids, continuation);
                            else __.response.setBody(getResponse(updateOptions.continuation, ids));
                            return;
                        } else throw new Error(err.number, "RetrySystemStoredProcedure");
                    }

                    // Apply each update operation to the retrieved document
                    var wasUpdated = false;
                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = false;
                        var result = handleUpdate(operation, doc, isCreate);
                        wasUpdated = wasUpdated || result.wasUpdated;
                        doc = result.doc;
                    }

                    var isAcceptedUpdate = true;
                    if (wasUpdated === false) {
                        processPendingDocs(ids, continuation);
                    }
                    else {
                        isAcceptedUpdate = __.replaceDocument(doc._self, doc, { etag: doc._etag }, updateCallback);
                    }

                    if (!isAcceptedUpdate) {
                        ids.unshift(id);
                        __.response.setBody(getResponse(updateOptions.continuation, ids));
                    }
                }

                function updateCallback(err) {
                    if (err) {
                        throw new Error(err.number, getError(err.number));
                    }

                    ++modifiedCount;
                    processPendingDocs(ids, continuation);
                }
            }

            function createDocument() {
                if (upsertDocument) {

                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = true;
                        var result = handleUpdate(operation, upsertDocument, isCreate);
                        upsertDocument = result.doc;
                    }

                    var isAcceptedCreate = __.createDocument(__.getSelfLink(), upsertDocument, createCallback);

                    if (updateOptions.returnModified) {
                        resultDocumentValue = upsertDocument;
                    }

                    if (!isAcceptedCreate) {
                        __.response.setBody(getResponse(updateOptions.continuation));
                        return;
                    }
                } else {
                    throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, "Upsert document should have a value."));
                }
            }

            function createCallback(err) {
                if (err) {
                    if (err.number === ErrorCodes.Conflict) {
                        __.response.setBody(getResponse(undefined));
                        throw new Error(ErrorCodes.BadRequest, "ResourceWithSpecifiedIdOrNameAlreadyExists");
                    }

                    throw err;
                }

                __.response.setBody(getResponse(undefined));
                return;
            }

            function handleUpdate(operation, doc, isCreate) {
                var parseDocument;
                var wasUpdated = true;
                if (updateOptions.isParseSystemCollection === true) {
                    parseDocument = doc;
                    doc = doc.value;

                    if (doc === undefined) {
                        doc = {};
                        doc[TypeMarker] = BsonType.Document;
                        doc[ValueMarker] = {};
                    }
                }

                var old_id = JSON.stringify(doc[ValueMarker]._id);
                try {
                    switch (operation.type) {
                        // Field Operations
                        case 'Inc':
                            fieldInc(doc, operation.field, operation.value);
                            break;
                        case 'Mul':
                            fieldMul(doc, operation.field, operation.value);
                            break;
                        case 'Min':
                            fieldMin(doc, operation.field, operation.value);
                            break;
                        case 'Max':
                            fieldMax(doc, operation.field, operation.value);
                            break;
                        case 'Rename':
                            fieldRename(doc, operation.field, operation.value);
                            break;
                        case 'Set':
                            wasUpdated = fieldSet(doc, operation.field, operation.value);
                            break;
                        case 'Unset':
                            fieldUnset(doc, operation.field);
                            break;
                        case 'Bit':
                            fieldBit(doc, operation.field, operation.value, operation.operator);
                            break;

                        // Array Operations
                        case 'AddToSet':
                            arrayAddToSet(doc, operation.field, operation.value);
                            break;
                        case 'Pop':
                            arrayPop(doc, operation.field, operation.value);
                            break;
                        case 'PullAll':
                            arrayPullAll(doc, operation.field, operation.value);
                            break;
                        case 'Pull':
                            arrayPull(doc, operation.field, operation.value);
                            break;
                        case 'Push':
                            arrayPush(doc, operation.field, operation.value, operation.slice, operation.position);
                            break;

                        // Full replace
                        case 'Replace':
                            doc = fullReplace(doc, operation.value);
                            break;

                        case 'SetOnInsert':
                            if (updateSpec.upsert === true && isCreate) {
                                fieldSet(doc, operation.field, operation.value);
                            }
                            break;

                        default:
                            throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidUpdateOperationType, operation.type));
                    }
                } catch (err) {
                    if (err.number == ChakraErrorCodes.JSERR_CantAssignToReadOnly) {
                        //  This can only be hit due to strict mode, as we don't explicitly use read-only properties for DML.
                        //  Convert to graceful error, to correspond to original behavior.
                        // Scenario (incorrect usage): insert({id:1, n:"foo"}); update({ id: 1 }, { $set: { 'n.x': 2 } });
                        // This error is handled in Gateway and converted to a write error.
                        throw new Error(ErrorCodes.BadRequest, "CannotUsePartOfPropertyToTraverseElement");
                    }
                    throw err;
                }

                if (!isCreate && old_id !== JSON.stringify(doc[ValueMarker]._id)) {
                    throw new Error(ErrorCodes.BadRequest, "CannotUpdateImmutableField");
                }

                if (updateOptions.isParseSystemCollection) {
                    parseDocument.value = doc;
                    doc = parseDocument;
                }

                return { wasUpdated: wasUpdated, doc: doc };
            }

            function removeDocument(doc) {
                var isAcceptedRemove = __.deleteDocument(doc._self, {}, removeCallback);
                resultDocumentValue = doc;
            }

            function removeCallback(err) {
                if (err) {
                    throw err;
                }

                __.response.setBody(getResponse(undefined));
                return;
            }

            function getDocIds(docs, startIndex) {
                if (!docs || startIndex >= docs.length) return undefined;

                var results = [];
                for (var i = startIndex; i < docs.length; ++i) {
                    if (querySpec.positionalUpdateExpr) {
                        results.push(docs[i][querySpec.queryRoot].id);
                    } else {
                        results.push(docs[i].id);
                    }
                }

                return results;
            }

            function isResultDocNeeded() {
                return updateOptions.isFindAndModify || updateOptions.remove;
            }

            function deepCopy(obj) {
                var copy;

                // Handle the 3 simple types, and null or undefined
                if (null == obj || "object" != typeof obj) return obj;

                // Handle Array
                if (obj instanceof Array) {
                    copy = [];
                    for (var i = 0, len = obj.length; i < len; i++) {
                        copy[i] = deepCopy(obj[i]);
                    }
                    return copy;
                }

                // Handle Object
                if (obj instanceof Object) {
                    copy = {};
                    for (var attr in obj) {
                        if (obj.hasOwnProperty(attr)) copy[attr] = deepCopy(obj[attr]);
                    }
                    return copy;
                }
            }

            const TypeMarker = "$t";
            const ValueMarker = "$v";
            const StringValueMarker = "$s";
            const RegExPattern = "$p";
            const RegExOptions = "$o";
            const JavaScriptCode = "$c";
            const JavaScriptScope = "$sc";

            // --------------------------------------
            // Field Operations
            // --------------------------------------

            // Operation: $inc
            //   Increments the value of the field by the specified amount.
            //   For now, Decimal128 can replace and be replaced by only Decimal128
            function fieldInc(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'object')

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    // if type is int64, if value exceeds Int64.Max/MinValue, limit to that.
                    if (value[TypeMarker] === BsonType.Int64) {
                        value[ValueMarker] = limitValueToOverflowSafeLimits(value[ValueMarker]);
                        value[StringValueMarker] = JSON.stringify(value[ValueMarker]);
                    }

                    doc[field] = value;
                } else {
                    var oldType = doc[field][TypeMarker];
                    var newType = value[TypeMarker];
                    if (isNumericBsonType(oldType)) {
                        if (isNumericBsonType(newType)) {
                            doc[field][TypeMarker] = getNumericType(doc[field][TypeMarker], value[TypeMarker]);
                            var sum = doc[field][ValueMarker] + value[ValueMarker];

                            // if type is int64, if value exceeds Int64.Max/MinValue, limit to that.
                            if (doc[field][TypeMarker] === BsonType.Int64) {
                                sum = limitValueToOverflowSafeLimits(sum);
                                doc[field][StringValueMarker] = JSON.stringify(sum);

                                //For Int32, in case of overflow, upgrade to Int64
                            } else if (doc[field][TypeMarker] === BsonType.Int32 && (sum > NumericalRanges.int32MaxValue || sum < NumericalRanges.int32MinValue)) {
                                doc[field][TypeMarker] = BsonType.Int64;
                                doc[field][StringValueMarker] = JSON.stringify(sum);

                                // For all other cases, delete StringValueMarker
                            } else {
                                delete doc[field][StringValueMarker];
                            }

                            doc[field][ValueMarker] = sum;
                        }
                        else if (newType === BsonType.Decimal128)
                            throw new Error(ErrorCodes.BadRequest, errorMessages.incompatDecimal128NonDecimal128);
                    }
                    else if (oldType === BsonType.Decimal128) {
                        if (newType === BsonType.Decimal128) {
                            var sum = addNormalizedStringRepresentations(doc[field][ValueMarker], value[ValueMarker]);
                            if (sum != null) {
                                doc[field][ValueMarker] = sum;
                                doc[field][StringValueMarker] = returnDolarSRepresentation(sum);
                            } else {
                                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.notValidDecimal128, "Result"));
                            }
                        }
                        else throw new Error(ErrorCodes.BadRequest, errorMessages.incompatDecimal128NonDecimal128);
                    }
                }
            }

            // Operation: $mul
            //   Multiplies the value of the field by the specified amount.
            function fieldMul(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'object')

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    // If property does not exist already, set $mul result to 0.
                    doc[field] = value;
                    doc[field][ValueMarker] = 0;
                    if (doc[field][TypeMarker] === BsonType.Int64) {
                        doc[field][StringValueMarker] = JSON.stringify(0);
                    }
                } else {
                    var oldType = doc[field][TypeMarker];
                    if (isNumericBsonType(oldType)) {
                        doc[field][TypeMarker] = getNumericType(doc[field][TypeMarker], value[TypeMarker]);
                        var product = doc[field][ValueMarker] * value[ValueMarker];

                        // if type is int64, if value exceeds Int64.MaxValue, limit to that.
                        if (doc[field][TypeMarker] === BsonType.Int64) {
                            product = limitValueToOverflowSafeLimits(product);
                            doc[field][StringValueMarker] = JSON.stringify(product);

                            //For Int32, in case of overflow, upgrade to Int64
                        } else if (doc[field][TypeMarker] === BsonType.Int32 &&
                            (product > NumericalRanges.int32MaxValue || product < NumericalRanges.int32MinValue)) {
                            doc[field][TypeMarker] = BsonType.Int64;
                            doc[field][StringValueMarker] = JSON.stringify(product);

                            // For all other cases, delete StringValueMarker
                        } else {
                            delete doc[field][StringValueMarker];
                        }

                        doc[field][ValueMarker] = product;
                    }
                }
            }

            // Operation: $min
            //   Only updates the field if the specified value is less than the existing field value.
            function fieldMin(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = value;
                } else {
                    var oldType = doc[field][TypeMarker];
                    var newType = value[TypeMarker];
                    //For now, Decimal128 can replace and be replaced by only Decimal128
                    if ((oldType === BsonType.Decimal128) != (newType === BsonType.Decimal128))
                        throw new Error(ErrorCodes.BadRequest, errorMessages.incompatDecimal128NonDecimal128);

                    if (compare(value, doc[field]) < 0) {
                        doc[field] = value;
                    }
                }
            }

            // Operation: $max
            //   Only updates the field if the specified value is greater than the existing field value.
            function fieldMax(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = value;
                } else {
                    var oldType = doc[field][TypeMarker];
                    var newType = value[TypeMarker];
                    // For now, Decimal128 can replace and be replaced by only Decimal128
                    if ((oldType === BsonType.Decimal128) != (newType === BsonType.Decimal128))
                        throw new Error(ErrorCodes.BadRequest, errorMessages.incompatDecimal128NonDecimal128);

                    if (compare(value, doc[field]) > 0) {
                        doc[field] = value;
                    }
                }
            }

            // Operation: $rename
            //   Renames a field.
            function fieldRename(doc, field, newField) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires((typeof newField+ == 'string')
                // Requires(newField+ != '')

                fieldUnset(doc, newField);
                var value = fieldUnset(doc, field);
                if (value !== undefined) {
                    fieldSet(doc, newField, value);
                }

                return value;
            }

            // Operation: $set
            //    Sets the value of a field in a document.
            function fieldSet(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];
                if (doc[field] !== undefined && compare(doc[field], value) == 0) {
                    return false;
                }
                else {
                    doc[field] = value;
                    return true;
                }
            }

            // Operation: $unset
            //   Removes the specified field from a document.
            function fieldUnset(doc, field) {
                // Requires(doc != undefined)
                // Requires(field != undefined)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                var value = undefined;
                if (doc !== undefined && doc[field] !== undefined) {
                    value = doc[field];
                    delete doc[field];
                }

                return value;
            }

            // Operation: $bit
            //   Performs bitwise AND, OR, and XOR updates of integer values.
            function fieldBit(doc, field, value, op) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')
                // Requires((op == 'And') || (op == 'Or') || (op == 'Xor'))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc !== undefined)
                    && doc[field] !== undefined
                    && (doc[field][TypeMarker] === BsonType.Int32 || doc[field][TypeMarker] === BsonType.Int64)) {
                    switch (op) {
                        case 'And':
                            doc[field][ValueMarker] &= value;
                            break;
                        case 'Or':
                            doc[field][ValueMarker] |= value;
                            break;
                        case 'Xor':
                            doc[field][ValueMarker] ^= value;
                            break;
                    }
                }
            }

            // --------------------------------------
            // Array Operations
            // --------------------------------------

            // Operation: $addToSet
            //  Adds elements to an array only if they do not already exist in the set.
            function arrayAddToSet(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = {};
                    doc[field][TypeMarker] = BsonType.Array;
                    doc[field][ValueMarker] = [];
                } else {
                    if (!Array.isArray(doc[field][ValueMarker]))
                        throw new Error(ErrorCodes.BadRequest, 'AddToSet operation requires a target array field.');
                }

                for (var i = 0; i < values.length; i++) {
                    if (!arrayContains(doc[field][ValueMarker], values[i])) {
                        doc[field][ValueMarker].push(values[i]);
                    }
                }
            }

            // Operation: $pop
            //  Removes the first or last item of an array.
            function arrayPop(doc, field, firstLast) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(firstLast == -1 OR firstLast == 1)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc !== undefined)
                    && doc[field] !== undefined
                    && Array.isArray(doc[field][ValueMarker])) {
                    if (firstLast == -1) {
                        doc[field][ValueMarker].shift();
                    } else if (firstLast == 1) {
                        doc[field][ValueMarker].pop();
                    }
                }
            }

            // Operation: $pullAll
            //  Removes all matching values from an array.
            function arrayPullAll(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc !== undefined)
                    && doc[field] !== undefined
                    && Array.isArray(doc[field][ValueMarker])) {
                    var array = doc[field][ValueMarker];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {
                        if (!arrayContains(values, array[i])) {
                            result.push(array[i]);
                        }
                    }

                    doc[field][ValueMarker] = result;
                }
            }

            // Operation: $pull
            //  Removes all array elements that match a specified query.
            function arrayPull(doc, field, value) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(filter != undefined)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc !== undefined)
                    && doc[field] !== undefined
                    && Array.isArray(doc[field][ValueMarker])) {
                    var array = doc[field][ValueMarker];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {

                        if (!matchExactly(array[i], value, true /*isArrayPull*/)) {
                            result.push(array[i]);
                        }
                    }

                    doc[field][ValueMarker] = result;
                }
            }

            // Operation: $push
            //  Adds an item to an array.
            function arrayPush(doc, field, values, sliceModifier, positionModifier) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = {};
                    doc[field][TypeMarker] = BsonType.Array;
                    doc[field][ValueMarker] = values;
                }
                else {
                    if (!Array.isArray(doc[field][ValueMarker]))
                        throw new Error(ErrorCodes.BadRequest, InternalErrors.PushOperatorRequiresTargetArray);

                    if (positionModifier !== undefined) {
                        for (var i = 0; i < values.length; i++) {
                            doc[field][ValueMarker].splice(positionModifier, 0, values[i]);
                            positionModifier++;
                        }
                    } else {
                        for (var i = 0; i < values.length; i++) {
                            doc[field][ValueMarker].push(values[i]);
                        }
                    }
                }

                if (sliceModifier !== undefined) {
                    if (sliceModifier < 0) {
                        doc[field][ValueMarker] = doc[field][ValueMarker].slice(sliceModifier);
                    } else {
                        doc[field][ValueMarker] = doc[field][ValueMarker].slice(0, sliceModifier);
                    }
                }
            }

            function fullReplace(doc, value) {
                var temp = doc;
                if (updateOperations.isParseSystemCollection === true) {
                    if (value[ValueMarker].hasOwnProperty("_id") && (JSON.stringify(doc.value[ValueMarker]._id) !== JSON.stringify(value[ValueMarker]._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc.value = value;
                    doc.value[ValueMarker].id = temp.value[ValueMarker].id;
                    doc.value[ValueMarker]._id = temp.value[ValueMarker]._id;
                }
                else {
                    if (value[ValueMarker].hasOwnProperty("_id") && (JSON.stringify(doc[ValueMarker]._id) !== JSON.stringify(value[ValueMarker]._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc = value;
                    doc[ValueMarker]._id = temp[ValueMarker]._id;
                }

                doc.id = temp.id;
                doc._self = temp._self;
                doc._etag = temp._etag;

                return doc;
            }


            // --------------------------------------
            // Common Utility Functions
            // --------------------------------------

            const BsonType = {
                EndOfDocument: 0,
                Double: 1,
                String: 2,
                Document: 3,
                Array: 4,
                Binary: 5,
                Undefined: 6,
                ObjectId: 7,
                Boolean: 8,
                DateTime: 9,
                Null: 10,
                RegularExpression: 11,
                JavaScript: 13,
                Symbol: 14,
                JavaScriptWithScope: 15,
                Int32: 16,
                Timestamp: 17,
                Int64: 18,
                Decimal128: 19,
                MaxKey: 127,
                MinKey: 255,
            }

            function isNumericBsonType(type) {
                if (type === BsonType.Double
                    || type === BsonType.Int32
                    || type === BsonType.Int64) {
                    return true;
                }

                return false;
            }

            // Returns the appropriate numeric type
            // for the update operation
            function getNumericType(oldType, newType) {

                if (oldType === BsonType.Double) {
                    return oldType;
                }

                if (oldType === BsonType.Int64) {
                    if (newType === BsonType.Double) {
                        return BsonType.Double;
                    }

                    return oldType;
                }

                // oldType is Int32
                if (newType === BsonType.Double || newType === BsonType.Int64) {
                    return newType;
                }

                return oldType;
            }

            function navigateTo(doc, path, upsert) {
                // Requires(doc !== undefined)
                // Requires(path !== undefined)

                var segments = path.split('.');

                var seg;
                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    seg = segments[i];

                    // We stop at the last path segment and return its value
                    if (i == (segments.length - 1)) break;

                    // If upsert is set and the segment does not exist then create it
                    if (upsert && (doc[seg] === undefined)) {
                        doc[seg] = {};
                        if (seg !== ValueMarker) {
                            doc[seg][TypeMarker] = BsonType.Document;
                        }
                    }

                    // Advance to the next segment
                    doc = doc[seg];
                }

                return [doc, seg];
            }

            function arrayContains(array, item) {
                // Requires(Array.isArray(array))
                // Requires(item !=== undefined)

                for (var i = 0; i < array.length; i++) {
                    if (matchExactly(array[i], item))
                        return true;
                }

                return false;
            }

            function compare(value1, value2) {
                // Requires(value1 !== undefined)
                // Requires(value2 !== undefined)

                var t1 = getTypeOrder(value1);
                var t2 = getTypeOrder(value2);

                if (t1 === t2) {
                    if (matchExactly(value1, value2)) return 0;
                    if (value1[ValueMarker] < value2[ValueMarker]) return -1;
                    return 1;
                }

                return t1 < t2 ? -1 : 1;
            }

            // Returns true if value1 exactly matches value2
            function matchExactly(value1, value2, isArrayPull) {
                if (value1 === undefined && value2 === undefined) return true;
                if (value1 === undefined || value2 === undefined) return false;
                if (isArrayPull === undefined) isArrayPull = false;

                // Mongo consider int32, int64 and double all the same when comparing values
                // same for String and Symbol.
                // So we get here typeOrder and compare values only if types match
                // getTypeOrder will return the same value for int32, int64 and double
                // and same value for string and symbol.
                // i.e: Int32: { $t: 16, $v: 5 } === Int64: { $t: 18, $v: 5 }
                var leftType = getTypeOrder(value1);
                var rightType = getTypeOrder(value2);

                // Compare types
                if (leftType !== rightType) {
                    return false;
                }

                // If types match, compare values
                var bsonType = value1[TypeMarker];
                switch (bsonType) {
                    case BsonType.Double:
                    case BsonType.String:
                    case BsonType.Binary:
                    case BsonType.ObjectId:
                    case BsonType.Boolean:
                    case BsonType.DateTime:
                    case BsonType.Symbol:
                    case BsonType.Int32:
                    case BsonType.Int64:
                    case BsonType.Timestamp:
                    case BsonType.Decimal128:
                        return value1[ValueMarker] === value2[ValueMarker];

                    // These Bson type will always match
                    case BsonType.Undefined:
                    case BsonType.Null:
                    case BsonType.MaxKey:
                    case BsonType.MinKey:
                        return true;

                    case BsonType.RegularExpression:
                        return value1[ValueMarker][RegExPattern] === value2[ValueMarker][RegExPattern]
                            && value1[ValueMarker][RegExOptions] === value2[ValueMarker][RegExOptions];

                    case BsonType.JavaScript:
                        return value1[ValueMarker][JavaScriptCode] === value2[ValueMarker][JavaScriptCode];

                    case BsonType.JavaScriptWithScope:
                        return value1[ValueMarker][JavaScriptCode] === value2[ValueMarker][JavaScriptCode]
                            && matchExactly(value1[ValueMarker][JavaScriptScope], value2[ValueMarker][JavaScriptScope], isArrayPull);

                    case BsonType.Document:
                        if (isArrayPull) {
                            if (Object.keys(value2[ValueMarker]).length === 0) {
                                return true;
                            } else {
                                for (var p in value2[ValueMarker]) {
                                    if (!value1[ValueMarker].hasOwnProperty(p)) {
                                        return false;
                                    }

                                    if (!matchExactly(value1[ValueMarker][p], value2[ValueMarker][p], isArrayPull)) {
                                        return false;
                                    }
                                }
                                return true;
                            }
                        } else {
                            if (Object.keys(value1[ValueMarker]).length !== Object.keys(value2[ValueMarker]).length)
                                return false;

                            for (var p in value1[ValueMarker]) {
                                if (!matchExactly(value2[ValueMarker][p], value1[ValueMarker][p], isArrayPull)) {
                                    return false;
                                }
                            }

                            return true;
                        }

                    case BsonType.Array:
                        if (value1[ValueMarker].length !== value2[ValueMarker].length)
                            return false;

                        for (var i = 0; i < value1[ValueMarker].length; i++) {
                            if (!matchExactly(value2[ValueMarker][i], value1[ValueMarker][i], isArrayPull)) {
                                return false;
                            }
                        }

                        return true;
                }

                throw new Error(ErrorCodes.BadRequest, "Corrupt data");
            }

            function getTypeOrder(value) {
                // Requires(value !== undefined)

                // Here is the type ordering 
                // 1.MinKey (internal type)
                // 2.Null
                // 3.Numbers (ints, longs, doubles)
                // 4.Symbol, String
                // 5.Object
                // 6.Array
                // 7.BinData
                // 8.ObjectId
                // 9.Boolean
                // 10.Date
                // 11.Timestamp
                // 11.Regular Expression
                // 12.MaxKey (internal type

                if (value[TypeMarker] === BsonType.MinKey) return 1;
                if (value[TypeMarker] === BsonType.Null) return 2;
                if (value[TypeMarker] === BsonType.Int32
                    || value[TypeMarker] === BsonType.Int64
                    || value[TypeMarker] === BsonType.Double) return 3;
                if (value[TypeMarker] === BsonType.String
                    || value[TypeMarker] === BsonType.Symbol) return 4;
                if (value[TypeMarker] === BsonType.Document) return 5;
                if (value[TypeMarker] === BsonType.Array) return 6;
                if (value[TypeMarker] === BsonType.Binary) return 7;
                if (value[TypeMarker] === BsonType.ObjectId) return 8;
                if (value[TypeMarker] === BsonType.Boolean) return 9;
                if (value[TypeMarker] === BsonType.DateTime) return 10;
                if (value[TypeMarker] === BsonType.Timestamp) return 11;
                if (value[TypeMarker] === BsonType.RegularExpression) return 12;
                if (value[TypeMarker] === BsonType.MaxKey) return 13;

                return 100;
            }
        }

        function deleteDocuments(query, limit, usePKContent) {
            var count = 0;
            var currentContinuation = '';

            var isAcceptedQuery = __.queryDocuments(__.getSelfLink(), query, { pageSize: 100 }, queryCallback);
            if (!isAcceptedQuery) throw new Error("Query without continuation is rejected.");

            function processCurrentContinuation() {
                if (limit > 0 && count >= limit) {
                    __.response.setBody({ count: count, done: true });
                } else if (currentContinuation) {
                    var isAcceptedQuery = __.queryDocuments(__.getSelfLink(), query, { pageSize: 100, continuation: currentContinuation }, queryCallback);
                    if (!isAcceptedQuery) {
                        __.response.setBody({ count: count, done: false });
                    }
                } else {
                    __.response.setBody({ count: count, done: true });
                }
            }

            function deleteNextDocument(documentArray, index) {
                // Delete the next document if there are more documents in the array and if the specified limit hasn't been reached 
                if (documentArray.length > index && (limit === 0 || count < limit)) {

                    var options = usePKContent ?
                        { partitionKeyContent: JSON.stringify(extractPKContent(documentArray[index])) } :
                        {};
                    var isAcceptedDelete = __.deleteDocument(
                        documentArray[index]._self,
                        options,
                        function (err, responseOptions) {
                            if (err) throw new Error('Error while deleting document: ' + err);
                            count++;

                            // Delete the next document in the array
                            deleteNextDocument(documentArray, index + 1);
                        });
                    if (!isAcceptedDelete) {
                        __.response.setBody({ count: count, done: false });
                    }
                } else {
                    processCurrentContinuation();
                }
            }

            function queryCallback(queryError, docFeed, responseOptions) {
                if (queryError) throw new Error('Error while querying documents: ' + queryError);

                currentContinuation = responseOptions.continuation;
                deleteNextDocument(docFeed, 0);
            }

            // Extracts part of content keeping only fields participating in PK.
            // We could extract PK here as well, but it's better to keep logic in one place - native side.
            function extractPKContent(content) {
                var pkDefinition = __.getPartitionKeyDefinition();  // Comes like this: [["name", "first"]].

                if (!pkDefinition || !Array.isArray(pkDefinition) ||
                    !pkDefinition[0] || !Array.isArray(pkDefinition[0]) || !pkDefinition[0].length)
                    throw new Error("Internal error. Partition key definition does not have enough fields.");
                if (pkDefinition.length > 1) throw new Error("Composite partition keys are not supported.");

                var current = content;
                var visited = [];
                for (var segment of pkDefinition[0]) {
                    if (typeof current === "object") {
                        visited.push(segment);
                        current = current[segment];
                    } else break;
                }

                var result = current;
                for (var i = visited.length - 1; i >= 0; --i) {
                    var outer = {};
                    outer[visited[i]] = result;
                    result = outer;
                }

                return result;
            }
        }

        // Need to use PK as part of CRUD: when PK is not provided in user request and the collection is partitioned.
        function needUsePKForDelete() {
            if (!getContext() || !getContext().getRequest()) throw new Error("Request must be available.");

            if (!getContext().getRequest().getHasPartitionKey()) {
                let pkDefinition = __.getPartitionKeyDefinition();
                return pkDefinition && pkDefinition.length > 0;
            }

            return false;
        }

        // System Function: commonDelete
        function commonDelete(querySpec) {
            if (arguments.length < 1)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.invalidFunctionCall, 'commonDelete', 1, arguments.length));
            if (!querySpec)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec must be specified'));
            if (querySpec.filterExpr && (typeof (querySpec.filterExpr) != 'string'))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec filter expression must be a valid string'));
            if (querySpec.orderByExpr && (typeof (querySpec.orderByExpr) != 'string'))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec orderby expression must be a valid string'));
            if (querySpec.limit && (typeof (querySpec.limit) != 'number'))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec limit must be a valid number'));
            if (((typeof querySpec.rootSymbol) != 'string') || (querySpec.rootSymbol === ''))
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query spec root symbol must be specified'));

            var usePKContent = needUsePKForDelete();
            var selector = usePKContent ? "*" : `${querySpec.rootSymbol}._self`;
            var queryString = `SELECT ${selector} FROM ${querySpec.rootSymbol}`;

            if (querySpec.filterExpr) {
                queryString += ' WHERE ' + querySpec.filterExpr;
            }

            if (querySpec.orderByExpr) {
                queryString += ' ORDER BY ' + querySpec.orderByExpr;
            }

            // When SqlParameters are provided, create a QuerySpec object, otherwise just pass the string.
            var queryObject;
            if (querySpec.sqlParameterCollection) {
                queryObject = {
                    query: queryString,
                    parameters: querySpec.sqlParameterCollection
                }
            } else {
                queryObject = queryString;
            }

            var count = 0;
            var currentContinuation = '';
            var limit = 0;

            if (querySpec.limit)
                limit = querySpec.limit;

            deleteDocuments(queryObject, limit, usePKContent);
        }

        // System Function: commonDeleteWithQueryString
        function commonDeleteWithQueryString(query, inputOptions) {
            if (arguments.length < 1)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.invalidFunctionCall, 'commonDeleteWithQueryString', 1, arguments.length));
            if (!query)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'Query must be specified'));

            var count = 0;
            var currentContinuation = '';
            var limit = 0;

            if (inputOptions && inputOptions.limit)
                limit = inputOptions.limit;

            deleteDocuments(query, limit, needUsePKForDelete());
        }
        //---------------------------------------------------------------------------------------------------
        function commonCount(filterExpression, rootSymbol, continuation, sqlParameters) {
            if (!rootSymbol)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidArgument, 'rootSymbol must be specified'));

            var queryString;
            if (filterExpression) {
                queryString = sprintf('SELECT VALUE 1 FROM %s WHERE %s', rootSymbol, filterExpression);
            } else {
                queryString = sprintf('SELECT VALUE 1 FROM %s', rootSymbol);
            }

            // When SqlParameters are provided, create a QuerySpec object, otherwise just pass the string.
            var queryObject;
            if (sqlParameters) {
                queryObject = {
                    query: queryString,
                    parameters: sqlParameters
                }
            } else {
                queryObject = queryString;
            }

            var isAccepted = __.queryDocuments(__.getSelfLink(), queryObject, { pageSize: -1, continuation: continuation }, queryCallback);

            if (!isAccepted) {
                __.response.setBody({ count: 0, continuation: continuation, requestAccepted: false });
            }

            function queryCallback(err, results, responseOptions) {
                if (err) throw new Error('Error while reading documents: ' + err);
                var count = results.length;

                __.response.setBody({ count: count, continuation: responseOptions.continuation, requestAccepted: true });
            }
        }
        
        //---------------------------------------------------------------------------------------------------
        /**
        * internalQueueAPI_getItem will operate on internal queue to fetch a single queue item from queue collections and set its visibility interval
        * @param  queryCount - Query count scope
        * @param  utcNow - UtcNow in ticks
        * @param  visibilityIntervalTicks - Result items' visibility interval in ticks
        */  
        function internalQueueAPI_getItem(queryCount, utcNow, visibilityIntervalTicks) {
            "use strict";
            implAsync().catch(ex => getContext().abort(ex));

            async function implAsync() {
                "use strict";
                var query = `SELECT TOP ${queryCount} * FROM c where c.nextVisibleTime < ${utcNow} ORDER BY c.nextVisibleTime`;

                var finalResult;
                var results = await queryDocumentsAsync(query, null, false);
                if (results.items.length > 0)
                {
                    var randomIndex = Math.floor(Math.random() * results.items.length);
                    var doc = results.items[randomIndex];
                    console.log(`QueueItem Id = ${doc.id} VisibilityTime = ${doc.nextVisibleTime} cosmos db creation time = ${doc._ts}`);
                    doc.dequeueCount++;
                    doc.nextVisibleTime = utcNow + visibilityIntervalTicks;
                    var updatedDoc = await replaceDocumentAsync(doc, { etag: doc._etag });
                    var itemArray = [updatedDoc];
                    finalResult = {items: itemArray};
                }

                var response = getContext().getResponse();
                response.setBody(finalResult);
            }
        }

        //---------------------------------------------------------------------------------------------------
        /**
        * internalQueueAPI_updateMessageVisibility will operate on internal queue to update the visibility interval of a queue item from queue collections
        * @param  messageId - Id of the queue message of which to update the message visibility.
        * @param  partitionKey - Partition key of the queue message
        * @param  utcNow - UtcNow in ticks
        * @param  visibilityIntervalTicks - Result items' visibility interval in ticks
        * @param  etag - Etag of queue message
        */
        function internalQueueAPI_updateMessageVisibility(messageId, partitionKey, nextVisibleTime, etag) {
            "use strict";
            implAsync().catch(ex => getContext().abort(ex));

            async function implAsync() {
                "use strict";
                var queryString = `SELECT * FROM c where c.id = ${messageId} and c.partitionKey = ${partitionKey}`;
                var finalResult;
                var results = await queryDocumentsAsync(queryString, null, true);
                if (results.items.length == 1) {
                    var doc = results.items[0];
                    console.log(`QueueItem Id = ${doc.id} VisibilityTime = ${doc.nextVisibleTime} cosmos db creation time = ${doc._ts}`);
                    doc.nextVisibleTime = nextVisibleTime;
                    var updatedDoc = await replaceDocumentAsync(doc, { etag: etag });
                    var itemArray = [updatedDoc];
                    finalResult = { items: itemArray };
                }
                else {
                    throw new Error(ErrorCodes.NotFound, "Could not find message with messageId " + messageId);
                }

                var response = getContext().getResponse();
                response.setBody(finalResult);
            }
        }
        
        //---------------------------------------------------------------------------------------------------
        /**
        * Bulk-patch documents in one batch. (Update handling functionality common to 'commonUpdate' sproc)
        * - The script sets response body to the number of docs patched and is supposed to be called as many
        *   times by the client until all the documents are patched.
        * @param  {Object[]} batch - Array of patches to be applied. Each patch has an id, pk value and an
        *                            array of patch operations to be applied where each patch operation
        *                            comprises operation type, field and value.
        * @param  {string}   pkDef - Partition key definition.
        * - The response consists of the count of documents patched and errorCode.
        */
        function bulkPatch(batch, pkDef) {
            if (!Array.isArray(batch)) throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.argumentMustBeArray, typeof batch));

            var collection = getContext().getCollection();
            var collectionLink = collection.getSelfLink();

            if (batch.length == 0) {
                setResponse(0);
                return;
            }

            // The count of patched docs, also used as current doc index.
            var patchedCount = 0;

            tryPatch(batch[patchedCount]);

            function tryPatch(patchItem, continuation) {
                var query = sprintf('select * from root r where r.id = \'%s\' and r.%s = \'%s\'', patchItem.id, pkDef, patchItem.pk);
                var requestOptions = { continuation: continuation };

                var isAcceptedQuery = collection.queryDocuments(collectionLink, query, requestOptions, queryCallback);
                if (!isAcceptedQuery) {
                    setResponse(patchedCount);
                    return;
                }
            }

            // Query callback
            function queryCallback(err, results, queryResponseOptions) {
                if (err) throw err;

                let resultIndex = 0;
                if (results.length === 0) {
                    if (queryResponseOptions.continuation) {
                        tryPatch(batch[patchedCount], queryResponseOptions.continuation);
                        return;
                    }
                    else {
                        setResponse(patchedCount, HttpStatusCode.NotFound);
                        return;
                    }
                }
                else {
                    processOneResult();
                }

                patchedCount++;
                if (patchedCount >= batch.length) {
                    // If we have patched all documents, we are done. Just set the response.
                    setResponse(patchedCount);
                } else {
                    // Patch next document.
                    tryPatch(batch[patchedCount]);
                }

                function processOneResult() {
                    var doc = results[resultIndex];
                    var updateOperations = batch[patchedCount].updates;

                    // Apply each update operation to the retrieved document
                    var result = null;
                    var wasUpdated = false;
                    for (var j = 0; j < updateOperations.length; j++) {
                        var operation = updateOperations[j];
                        var isCreate = false;

                        try {
                            result = handleUpdate(operation, doc, isCreate);
                            wasUpdated = wasUpdated || result.wasUpdated;
                            doc = result.doc;
                        } catch (err) {
                            throw err;
                        }
                    }
                    
                    if (wasUpdated)
                    {
                        let isAcceptedUpdate = collection.replaceDocument(doc._self, doc, { etag: doc._etag }, updateCallback);
                        if (!isAcceptedUpdate) {
                            setResponse(patchedCount);
                            return;
                        }
                    }
                    else
                    {
                        updateCallback(undefined);
                    }
                }

                function updateCallback(err) {
                    if (err) {
                        throw new Error(err.number, getError(err.number));
                    }

                    resultIndex++;
                    if (resultIndex < results.length) {
                        processOneResult();
                    }
                }
            }

            function handleUpdate(operation, doc, isCreate) {
                var wasUpdated = true;
                var old_id = JSON.stringify(doc._id);

                try {
                    switch (operation.type) {
                        // Field Operations
                        case 'Inc':
                            fieldInc(doc, operation.field, operation.value);
                            break;
                        case 'Mul':
                            fieldMul(doc, operation.field, operation.value);
                            break;
                        case 'Min':
                            fieldMin(doc, operation.field, operation.value);
                            break;
                        case 'Max':
                            fieldMax(doc, operation.field, operation.value);
                            break;
                        case 'Rename':
                            fieldRename(doc, operation.field, operation.value);
                            break;
                        case 'Set':
                            wasUpdated = fieldSet(doc, operation.field, operation.value);
                            break;
                        case 'Unset':
                            fieldUnset(doc, operation.field);
                            break;
                        case 'CurrentDate':
                            fieldCurrentDate(doc, operation.field, operation.value);
                            break;
                        case 'Bit':
                            fieldBit(doc, operation.field, operation.value, operation.operator);
                            break;

                            // Array Operations
                        case 'AddToSet':
                            arrayAddToSet(doc, operation.field, operation.value);
                            break;
                        case 'Pop':
                            arrayPop(doc, operation.field, operation.value);
                            break;
                        case 'PullAll':
                            arrayPullAll(doc, operation.field, operation.value);
                            break;
                        case 'Pull':
                            arrayPull(doc, operation.field, operation.value);
                            break;
                        case 'Push':
                            arrayPush(doc, operation.field, operation.value);
                            break;
                        case 'Remove':
                            arrayRemove(doc, operation.field, operation.value);
                            break;

                            // Full replace
                        case 'Replace':
                            doc = fullReplace(doc, operation.value);
                            break;

                        case 'SetOnInsert':
                            if (updateSpec.upsert === true && isCreate) {
                                fieldSet(doc, operation.field, operation.value);
                            }
                            break;

                        default:
                            throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.commonDml_invalidUpdateOperationType, operation.type));
                    }
                } catch (err) {
                    if (err.number == ChakraErrorCodes.JSERR_CantAssignToReadOnly) {
                        //  This can only be hit due to strict mode, as we don't explicitly use read-only properties for DML.
                        //  Convert to graceful error, to correspond to original behavior.
                        // Scenario (incorrect usage): insert({id:1, n:"foo"}); update({ id: 1 }, { $set: { 'n.x': 2 } });
                        // This error is handled in Gateway and converted to a write error.
                        throw new Error(ErrorCodes.BadRequest, "CannotUsePartOfPropertyToTraverseElement");
                    }
                    throw err;
                }

                if (old_id !== JSON.stringify(doc._id) && !isCreate) {
                    throw new Error(ErrorCodes.BadRequest, "CannotUpdateImmutableField");
                }

                return { wasUpdated: wasUpdated, doc: doc };
            }

            // --------------------------------------
            // Field Operations
            // --------------------------------------

            // Operation: $inc
            //   Increments the value of the field by the specified amount.
            function fieldInc(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    if ((typeof oldValue) == "number")
                        return oldValue + value;
                    return oldValue;
                });
            }

            // Operation: $mul
            //   Multiplies the value of the field by the specified amount.
            function fieldMul(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return 0;
                    if ((typeof oldValue) == "number")
                        return oldValue * value;
                    return oldValue;
                });
            }

            // Operation: $min
            //   Only updates the field if the specified value is less than the existing field value.
            function fieldMin(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    return compare(value, oldValue) < 0 ? value : oldValue;
                });
            }

            // Operation: $max
            //   Only updates the field if the specified value is greater than the existing field value.
            function fieldMax(doc, field, value) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)

                fieldSet(doc, field, value, function (oldValue) {
                    if (oldValue === undefined)
                        return value;
                    return compare(value, oldValue) > 0 ? value : oldValue;
                });
            }

            // Operation: $rename
            //   Renames a field.
            function fieldRename(doc, field, newField) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires((typeof newField+ == 'string')
                // Requires(newField+ != '')

                var value = fieldUnset(doc, field);
                if (value !== undefined) {
                    fieldSet(doc, newField, value);
                }

                return value;
            }

            // Operation: $set
            //    Sets the value of a field in a document.
            function fieldSet(doc, field, value, applyOp) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(value !== undefined)
                // Requires((applyOp === undefined) || (typeof(applyOp) == 'function'))

                var navResult = navigateTo(doc, field, true);

                doc = navResult[0];
                field = navResult[1];

                if (value === doc[field] && !applyOp) {
                    return false;
                }
                else {
                    doc[field] = value;
                    return true;
                }
            }

            // Operation: $unset
            //   Removes the specified field from a document.
            function fieldUnset(doc, field) {
                // Requires(doc != undefined)
                // Requires(field != undefined)

                var segments = field.split('.');
                var value;

                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    var seg = segments[i];

                    // if this is the last segment then we delete the field
                    if (i == (segments.length - 1)) {
                        value = doc[seg];
                        delete doc[seg];
                    } else {
                        // Advance to the next segment
                        doc = doc[seg];
                    }
                }

                return value;
            }

            // Operation: $currentDate
            //   Sets the value of a field to current date, either as a Date or a Timestamp.
            function fieldCurrentDate(doc, field, type) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires((type == 'date') || (type == 'timestamp'))

                var value = type === 'date' ?
                    { $date: new Date().toISOString() } :
                    { $timestamp: { t: Math.floor(new Date().getTime() / 1000), i: 1 } };

                fieldSet(doc, field, value);
            }

            // Operation: $bit
            //   Performs bitwise AND, OR, and XOR updates of integer values.
            function fieldBit(doc, field, value, op) {
                // Requires(doc !== undefined)
                // Requires(field !== undefined)
                // Requires(typeof(value) == 'number')
                // Requires((op == 'And') || (op == 'Or') || (op == 'Xor'))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if (doc && ((typeof doc[field]) == 'number')) {
                    switch (op) {
                        case 'And':
                            doc[field] &= value;
                            break;
                        case 'Or':
                            doc[field] |= value;
                            break;
                        case 'Xor':
                            doc[field] ^= value;
                            break;
                    }
                }
            }

            // --------------------------------------
            // Array Operations
            // --------------------------------------

            // Operation: $addToSet
            //  Adds elements to an array only if they do not already exist in the set.
            function arrayAddToSet(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = new Array();
                } else {
                    if (!Array.isArray(doc[field]))
                        throw new Error(ErrorCodes.BadRequest, 'AddToSet operation requires a target array field.');
                }

                for (var i = 0; i < values.length; i++) {
                    if (!arrayContains(doc[field], values[i])) {
                        doc[field].push(values[i]);
                    }
                }
            }

            // Operation: $pop
            //  Removes the first or last item of an array.
            function arrayPop(doc, field, firstLast) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(firstLast == -1 OR firstLast == 1)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    if (firstLast == -1) {
                        doc[field].shift();
                    } else if (firstLast == 1) {
                        doc[field].pop();
                    }
                }
            }

            // Operation: $pullAll
            //  Removes all matching values from an array.
            function arrayPullAll(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    var array = doc[field];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {
                        if (!arrayContains(values, array[i])) {
                            result.push(array[i]);
                        }
                    }

                    doc[field] = result;
                }
            }

            // Operation: $pull
            //  Removes all array elements that match a specified query.
            function arrayPull(doc, field, value) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(filter != undefined)

                var navResult = navigateTo(doc, field, false);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    var array = doc[field];
                    var result = [];

                    for (var i = 0; i < array.length; i++) {
                        if (!arrayPullFilter(array[i], value)) {
                            result.push(array[i]);
                        }
                    }

                    doc[field] = result;
                }
            }

            // Operation: $push
            //  Adds an item to an array.
            //  TODO: RCA positionModifier and sliceModifier (https://msdata.visualstudio.com/CosmosDB/_workitems/edit/142123)
            function arrayPush(doc, field, values) {
                // Requires(doc != undefined)
                // Requires(field != undefined)
                // Requires(Array.isArray(values))

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if (doc[field] === undefined) {
                    doc[field] = values;
                }
                else {
                    if (!Array.isArray(doc[field])) {
                        throw new Error(ErrorCodes.BadRequest, InternalErrors.PushOperatorRequiresTargetArray);
                    }

                    for (var i = 0; i < values.length; i++) {
                        doc[field].push(values[i]);
                    }
                }
            }

            // Removes an item from an array.
            function arrayRemove(doc, field, value) {
                // Requires(doc != undefined)
                // Requires(field != undefined)

                var navResult = navigateTo(doc, field, true);
                doc = navResult[0];
                field = navResult[1];

                if ((doc != undefined) && Array.isArray(doc[field])) {
                    var array = doc[field];
                    var index = array.indexOf(value);
                    if (index > -1) {
                        array.splice(index, 1);
                    }
                }
            }

            function fullReplace(doc, value) {
                var temp = doc;
                if (updateOptions.isParseSystemCollection === true) {
                    if (value.hasOwnProperty("_id") && (JSON.stringify(doc.value._id) !== JSON.stringify(value._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc.value = value;
                    doc.value.id = temp.value.id;
                    doc.value._id = temp.value._id;
                }
                else {
                    if (value.hasOwnProperty("_id") && (JSON.stringify(doc._id) !== JSON.stringify(value._id))) {
                        throw new Error(ErrorCodes.BadRequest, "CannotReplaceImmutableField");
                    }

                    doc = value;
                    doc._id = temp._id;
                }

                doc.id = temp.id;
                doc._self = temp._self;
                doc._etag = temp._etag;

                return doc;
            }


            // --------------------------------------
            // Common Utility Functions
            // --------------------------------------
            function navigateTo(doc, path, upsert) {
                // Requires(doc !== undefined)
                // Requires(path !== undefined)

                var segments = path.split('.');

                var seg;
                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    seg = segments[i];

                    // We stop at the last path segment and return its value
                    if (i == (segments.length - 1)) break;

                    // If upsert is set and the segment does not exist then create it
                    if (upsert && (doc[seg] === undefined)) {
                        doc[seg] = {};
                    }

                    // Advance to the next segment
                    doc = doc[seg];
                }

                return [doc, seg];
            }

            function arrayContains(array, item) {
                // Requires(Array.isArray(array))
                // Requires(item !=== undefined)

                if ((typeof item) == 'object') {
                    for (var i = 0; i < array.length; i++) {
                        if (compare(array[i], item) == 0)
                            return true;
                    }

                    return false;
                }

                return array.indexOf(item) >= 0;
            }

            function objectsEquivalent(left, right, depth) {
                if (Object.getPrototypeOf(left) !== Object.getPrototypeOf(right)) return false;

                var leftPropertyNames = Object.getOwnPropertyNames(left);
                var rightPropertyNames = Object.getOwnPropertyNames(right);

                if (leftPropertyNames.length != rightPropertyNames.length) {
                    return false;
                }

                for (var i = 0; i < leftPropertyNames.length; i++) {
                    var leftProp = leftPropertyNames[i];
                    var rightProp = rightPropertyNames[i];

                    // Mongo behavior: {a: 1, b: 2} != {b: 2, a: 1}
                    if (leftProp !== rightProp) {
                        return false;
                    }

                    if (typeof (left[leftProp]) == 'object') {
                        if (compare(left[leftProp], right[leftProp], depth + 1) != 0) {
                            return false;
                        }
                    } else {
                        if (left[leftProp] !== right[leftProp]) {
                            return false;
                        }
                    }
                }
                return true;
            }

            function arraysEquivalent(left, right, depth) {
                if (left === right) return true;
                if (left === null || right === null) return false;
                if (left.length != right.length) return false;

                if (Object.getOwnPropertyNames(left).length > left.length + 1 || Object.getOwnPropertyNames(right).length > right.length + 1) {
                    return objectsEquivalent(left, right, depth);
                }

                for (var i = 0; i < left.length; i++) {
                    if (compare(left[i], right[i], depth + 1) != 0) {
                        return false;
                    }
                }
                return true;
            }

            function compare(value1, value2, depth) {
                // Requires(value1 !== undefined)
                // Requires(value2 !== undefined)

                // To prevent infinite object property reference loop
                if (depth === undefined) depth = 1;
                if (depth > 1000) return false;

                var t1 = getTypeOrder(value1);
                var t2 = getTypeOrder(value2);

                if (t1 === t2) {
                    if (value1 == value2) return 0;

                    if (Array.isArray(value1)) {
                        if (arraysEquivalent(value1, value2, depth)) return 0;
                    }
                    else if (typeof value1 == 'object') {
                        if (objectsEquivalent(value1, value2, depth)) return 0;
                    } else {
                        return (value1 < value2) ? -1 : 1;
                    }

                    return (value1 < value2) ? -1 : 1;
                }

                return t1 < t2 ? -1 : 1;
            }

            // If the specified <value> to remove is an array, $pull removes only the elements in the array that match the
            // specified <value> exactly, including order. If the specified <value> to remove is a document, $pull removes only
            // the elements in the array that have/[contain] the exact same fields and values. The ordering of the fields can differ.
            // Note: Mongo ignores the pull operator when the item to be removed (pullItem) is [] or {}.
            let arrayPullFilter = function (arrayItem, pullItem) {
                if (typeof pullItem == 'object' && pullItem !== null) {
                    if (typeof arrayItem == 'object' && arrayItem !== null) {
                        if (Array.isArray(arrayItem)) { // Array [] case
                            if (Array.isArray(pullItem) && (pullItem.length === arrayItem.length)) {
                                for (var i = 0; i < arrayItem.length; i++) {
                                    if (typeof pullItem[i] == 'object') {
                                        if (!arrayPullFilter(arrayItem[i], pullItem[i])) {
                                            return false;
                                        }
                                    } else {
                                        if (pullItem[i] !== arrayItem[i]) {
                                            return false;
                                        }
                                    }
                                }
                                return true;
                            } else {
                                return false;
                            }
                            return true;
                        } else { // Object {} case
                            if (Array.isArray(pullItem)) return false;
                            if (Object.keys(pullItem).length == 0) return true;

                            for (var p in pullItem) {
                                if (typeof pullItem[p] == 'object' && arrayItem.hasOwnProperty(p)) {
                                    if (!arrayPullFilter(arrayItem[p], pullItem[p])) {
                                        return false;
                                    }
                                } else {
                                    if (!arrayItem.hasOwnProperty(p) || !(arrayItem[p] === pullItem[p])) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        }
                    } else {
                        return false;
                    }
                }
                else {
                    return arrayItem === pullItem;
                }
            }

            function getTypeOrder(value) {
                // Requires(value !== undefined)

                // Here is the type ordering 
                // 1.MinKey (internal type)
                // 2.Null
                // 3.Numbers (ints, longs, doubles)
                // 4.Symbol, String
                // 5.Object
                // 6.Array
                // 7.BinData
                // 8.ObjectId
                // 9.Boolean
                // 10.Date, Timestamp
                // 11.Regular Expression
                // 12.MaxKey (internal type)

                switch (typeof value) {
                    case "number":
                        return 3;
                    case "string":
                    case "symbol":
                        return 4;
                    case "boolean":
                        return 9;
                    case "object":
                        if (value === null) return 2;
                        if (Array.isArray(value)) return 6;
                        return 5;
                    default:
                        return 12;
                }
            }

            function setResponse(count, err) {
                __.response.setBody({
                    count: count,
                    errorCode: err ? err : 0
                });
            }
        }
        //---------------------------------------------------------------------------------------------------
        /**
        * Bulk group-by documents.
        * - The script aggregates documents sharing the same group-by key value and is supposed to be called as many
        *   times by the client until all the documents are read (errorCode = 1).
        * @param  {string}   groupByKeyDef - Definition of the key to group-by.
        * @param  {string}   continuationGroupByKey - Value of group-by key to continue from.
        * @param  {string}   minExclusiveSplitRangeKey - Minimum (exclusive) value of current split range used for intra-partition parallelism.
        * @param  {string}   maxInclusiveSplitRangeKey - Maximum (inclusive) value of current split range used for intra-partition parallelism.
        * @param  {string}   continuationToken - Continuation token from previous script invocation.
        * @param  {int}      maxBatchCount - Maximum count of aggregated documents returned from script.     
        * - The response consists of the grouped documents read, continuation group-by key value, continuation token and errorCode.
        */
        function bulkGroupBy(groupByKeyDef, continuationGroupByKey, minExclusiveSplitRangeKey, maxInclusiveSplitRangeKey, continuationToken, maxBatchCount) {

            var collection = getContext().getCollection();
            var collectionLink = collection.getSelfLink();

            var readResults = [];
            var boundaryGroupByKey = continuationGroupByKey;
            var batchIndex = 0;

            tryRead(continuationToken);

            function tryRead(continuation) {
                var query = sprintf('select * from root r where r.%s > \'%s\' and r.%s <= \'%s\'', groupByKeyDef, minExclusiveSplitRangeKey, groupByKeyDef, maxInclusiveSplitRangeKey);
                if (continuationGroupByKey) {
                    query += sprintf(' and r.%s > \'%s\'', groupByKeyDef, continuationGroupByKey);
                }
                query += sprintf(' order by r.%s', groupByKeyDef);

                var requestOptions = { continuation: continuation };

                var isAcceptedQuery = collection.queryDocuments(collectionLink, query, requestOptions, queryCallback);
                if (!isAcceptedQuery) {
                    // Query not accepted, have to yield & retry same (continuationGroupByKey, continuation).
                    setResponse(readResults, boundaryGroupByKey, continuation, 2);
                    return;
                }
            }

            // Query callback
            function queryCallback(err, results, queryResponseOptions) {
                if (err) throw err;

                let resultIndex = 0;
                if (results.length === 0) {
                    if (queryResponseOptions.continuation) {
                        tryRead(queryResponseOptions.continuation);
                        return;
                    }
                    else {
                        // Reached end of partition - query returned no results (no retry).
                        setResponse(readResults, boundaryGroupByKey, null, 1);
                        return;
                    }
                }
                else {
                    while (batchIndex < maxBatchCount) {
                        processOneBatch();
                        batchIndex++;

                        if (resultIndex == results.length) {
                            if (queryResponseOptions.continuation) {
                                tryRead(queryResponseOptions.continuation);
                                return;
                            }
                            else {
                                // Reached end of current query result set - execute sproc for (next pkBoundary, empty continuationToken).
                                setResponse(readResults, boundaryGroupByKey, null, 4);
                                return;
                            }
                        }
                    }

                    // Finished reading maxBatchCount batches - execute sproc for (next pkBoundary, empty continuationToken).
                    setResponse(readResults, boundaryGroupByKey, null, 3);
                    return;
                }

                // A batch refers to documents sharing same partition key value.
                function processOneBatch() {
                    var groupedDoc = {};

                    var doc = results[resultIndex];
                    var navResult = navigateTo(doc, groupByKeyDef, false);
                    var innerDoc = navResult[0];
                    var pkField = navResult[1];
                    var pkValue = innerDoc[pkField];

                    groupedDoc[doc['id']] = doc;
                    resultIndex++;

                    while (resultIndex < results.length) {
                        var nextDoc = results[resultIndex];
                        var nextDocNavResult = navigateTo(nextDoc, groupByKeyDef, false);
                        var nextDocInnerDoc = nextDocNavResult[0];
                        var nextDocpkField = nextDocNavResult[1];
                        var nextDocPkValue = nextDocInnerDoc[nextDocpkField];

                        if (pkValue === nextDocPkValue) {
                            groupedDoc[nextDoc['id']] = nextDoc;
                            resultIndex++;
                        }
                        else {
                            readResults.push(groupedDoc);
                            boundaryGroupByKey = pkValue;
                            return;
                        }
                    }

                    if (resultIndex === results.length) {
                        readResults.push(groupedDoc);
                        boundaryGroupByKey = pkValue;
                        return;
                    }
                }
            }

            // Utility functions.

            function navigateTo(doc, path, upsert) {
                // Requires(doc !== undefined)
                // Requires(path !== undefined)

                var segments = path.split('.');

                var seg;
                for (var i = 0; i < segments.length; i++) {
                    if (doc === undefined) break;

                    seg = segments[i];

                    // We stop at the last path segment and return its value
                    if (i == (segments.length - 1)) break;

                    // If upsert is set and the segment does not exist then create it
                    if (upsert && (doc[seg] === undefined)) {
                        doc[seg] = {};
                    }

                    // Advance to the next segment
                    doc = doc[seg];
                }

                return [doc, seg];
            }

            function setResponse(results, boundaryGroupByKey, continuationToken, err) {
                __.response.setBody({
                    readResults: results,
                    continuationGroupByKey: boundaryGroupByKey,
                    continuationToken: continuationToken,
                    errorCode: err ? err : 0
                });
            }
        }
        //---------------------------------------------------------------------------------------------------
        /**
        * Bulk-import documents in one batch.
        * - The script sets response body to the number of docs imported and is supposed called multiple times 
        *   by the client until total number of docs desired by the client is imported.
        * - If there is conflict, the script stops and reports to the client. Transaction is not aborted, docs inserted so far stay. 
        * @param  {Object[]} docs - Array of documents to import. All documents MUST have ids assigned. Could be objects or strings.
        * @param  {Object}   inputOptions - optional Options object, in addition to common options, 
        *                    can have: softStopOnConflict: true/false systemCollectionId: string.
        *                    consider setting disableAutomaticIdGeneration to true.
        */
        function commonBulkInsert(docs, inputOptions) {
            if (!Array.isArray(docs)) throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.argumentMustBeArray, typeof docs));
            if (inputOptions && inputOptions.systemCollectionId && typeof inputOptions.systemCollectionId !== "string") {
                throw new Error(ErrorCodes.BadRequest, systemCollectionIdMustBeString);
            }

            var collection = getContext().getCollection();
            var collectionLink = collection.getSelfLink();

            if (docs.length == 0) {
                setResponse(0, 0, 0);
                return;
            }

            // The count of imported docs, also used as current doc index.
            var totalCount = 0;

            // The count of imported docs which were newly created.
            var createdCount = 0;

            // The count of imported docs which were replace operations on pre-existing documents.
            var replacedCount = 0;

            // Call the CRUD API to create a document.
            tryCreate(docs[totalCount]);

            // Note that there are 2 normal exit conditions:
            // 1) The createDocument request was not accepted. 
            //    In this case the callback will not be called, we just call setBody and we are done.
            // 2) The callback was called docs.length times.
            //    In this case all documents were created and we don't need to call tryCreate anymore. Just set the response and we are done.
            function tryCreate(doc) {
                if (inputOptions && inputOptions.systemCollectionId) {
                    if (typeof doc !== "object") throw new Error(ErrorCodes.BadRequest, docsMustBeObjectForSystemCollectionId);
                    var wrappedDoc = {
                        collectionName: inputOptions.systemCollectionId,
                        value: doc
                    }

                    if (inputOptions.enableBsonSchema === true) {
                        wrappedDoc.id = inputOptions.systemCollectionId + "_" + JSON.stringify(doc["$v"]._id["$v"]);
                    } else {
                        wrappedDoc.id = inputOptions.systemCollectionId + "_" + JSON.stringify(doc._id);
                    }

                    doc = wrappedDoc;
                }

                try {
                    var isAccepted = false;
                    if (inputOptions && inputOptions.enableUpsert) {
                        isAccepted = collection.upsertDocument(collectionLink, doc, inputOptions, createCallback);
                    } else {
                        isAccepted = collection.createDocument(collectionLink, doc, inputOptions, createCallback);
                    }
                    if (!isAccepted) setResponse(totalCount, createdCount, replacedCount);
                } catch (err) {
                    if (inputOptions && inputOptions.softStopOnBadRequest && err.number == HttpStatusCode.BadRequest) {
                        setResponse(totalCount, createdCount, replacedCount, err, doc);
                    }
                    else throw err;
                }
            }

            function createCallback(err, doc, options) {
                if (err) {
                    if (inputOptions && inputOptions.softStopOnConflict && err.number == HttpStatusCode.Conflict) {
                        setResponse(totalCount, createdCount, replacedCount, err);
                    }
                    else throw err;
                }
                else {
                    // One more document has been inserted, increment the insertedCount.
                    totalCount++;

                    if (options.statusCode == HttpStatusCode.Created) {
                        createdCount++;
                    } else {
                        replacedCount++;
                    }

                    if (totalCount >= docs.length) {
                        // If we have created all documents, we are done. Just set the response.
                        setResponse(totalCount, createdCount, replacedCount);
                    } else {
                        // Create next document.
                        tryCreate(docs[totalCount]);
                    }
                }
            }

            function setResponse(count, createdCount, replacedCount, err, badDoc) {
                __.response.setBody({
                    count: count,
                    createdCount: createdCount,
                    replacedCount: replacedCount,
                    errorCode: err ? err.number : 0,
                    failedDoc: badDoc ? badDoc : null
                });
            }
        }
        /**
         * Tries to find a document with nodeName specified by the nodeName parameter, and if not found, finds 
         * random document without nodeName set (i.e. undefined/null/empty string) and sets nodeName to the given value. 
         * If cannot find such a document, throws error with status = HttpStatusCode.InternalError.
         * Sets response body to the resulting document.
         * @name getOrUpdateAgentConfig
         * @function
         * @param {nodeName} collectionLink - resource link of the collection whose documents are being queried
         operation will be thrown.</p>
         */
        function getOrUpdateAgentConfig(nodeName) {
            if (!nodeName) throw new Error(HttpStatusCode.BadRequest, "A non-empty nodeName parameter must be provided.");

            tryQuery();

            function tryQuery(continuation) {
                const query = {
                    query: "SELECT * from c WHERE c.nodeName = @nodeName",
                    parameters: [{ name: '@nodeName', value: nodeName }]
                };
                var isAccepted = __.queryDocuments(
                    __.getSelfLink(),
                    query, 
                    { continuation },
                    (err, feed, options) => {
                        if (err) throw err;

                        if ((!feed || !feed.length) && options.continuation) {
                            tryQuery(options.continuation);
                        } else if (feed && feed.length > 0) {
                            if (feed.length > 1) throw new Error(HttpStatusCode.InternalError, `Data is inconsistent: more that one doc with nodeName=$'nodeName' exists.`);
                            var uploadAgentConfig = feed[0];
                            setResponseBody(uploadAgentConfig, getStorageAccountUriToKey(uploadAgentConfig));
                        } else {
                            tryReplace();
                        }
                    });
                if (!isAccepted) throw new Error(HttpStatusCode.NotAccepted, "The root query was not accepted.");
            }

            function tryReplace(continuation) {
                var isAccepted = __.queryDocuments(
                    __.getSelfLink(),
                    "SELECT * from c WHERE c.nodeName = '' OR IS_NULL(c.nodeName) OR NOT IS_DEFINED(c.nodeName)", 
                    { continuation },
                    (err, feed, options) => {
                        if (err) throw err;

                        if (!feed || !feed.length) {
                            if (!options.continuation) throw new Error(HttpStatusCode.InternalError, "Could not find a document without a nodeName.");
                            tryReplace(options.continuation);
                        } else {
                            var uploadAgentConfig = feed[getRandomIndex(feed.length)];
                            uploadAgentConfig.nodeName = nodeName;
                            isAccepted = __.replaceDocument(uploadAgentConfig._self, uploadAgentConfig, (err, uploadAgentConfig) => {
                                if (err) throw err;
                                setResponseBody(uploadAgentConfig, getStorageAccountUriToKey(uploadAgentConfig));
                            });
                            if (!isAccepted) throw new Error(HttpStatusCode.NotAccepted, "The replace was not accepted.");
                        }
                    });
                if (!isAccepted) throw new Error(HttpStatusCode.NotAccepted, "The inner query was not accepted.");
            }

            // Returns a pseudo-random whole number in the range of [0, max). Assumes max > 0.
            function getRandomIndex(max) {
                return Math.floor(Math.random() * max);
            }

            function getStorageAccountUriToKey(uploadAgentConfig) {
                var storageAccountUriToKey = new Object();
                storageAccountUriToKey[uploadAgentConfig.ssTableUploadLocation.storageAccountUri] = __[getStorageAccountKeyFunctionName](uploadAgentConfig.ssTableUploadLocation.storageAccountUri);
                storageAccountUriToKey[uploadAgentConfig.cdcUploadLocation.storageAccountUri] = __[getStorageAccountKeyFunctionName](uploadAgentConfig.cdcUploadLocation.storageAccountUri);
                return storageAccountUriToKey;
            }

            function setResponseBody(uploadAgentConfig, storageAccountUriToKey) {
                __.response.setBody({ 
                    uploadAgentConfig,
                    storageAccountUriToKey
                });
            }
        }

        function getStorageAccountKey(storageAccountUri) {
            if (!storageAccountUri) throw new Error(HttpStatusCode.BadRequest, "A non-empty storageAccountUri parameter must be provided.");
            var storageAccountKey = __[getStorageAccountKeyFunctionName](storageAccountUri);
            __.response.setBody(storageAccountKey);
        }

        //---------------------------------------------------------------------------------------------------
        // System function: echo.
        // Used mainly for testing. It's read-only but it doesn't make sense to create a .js file just for this/testing.
        function echo(arg) {
            console.log("echo: " + arg);
            if (typeof __.response != "undefined") __.response.setBody(arg);
            return arg;
        }
        //---------------------------------------------------------------------------------------------------
        // Like C sprintf, currently only works for %s and %%.
        // Example: sprintf('Hello %s!', 'World!') => 'Hello, World!'
        function sprintf(format) {
            var args = arguments;
            var i = 1;
            return format.replace(/%((%)|s)/g, function (matchStr, subMatch1, subMatch2) {
                // In case of %% subMatch2 would be '%'.
                return subMatch2 || args[i++];
            });
        }
        //---------------------------------------------------------------------------------------------------
        //---------------------------------------------------------------------------------------------------
        // The functions below are for supporting Decimal128.
        /*
        Our backend does not natively support decimal128 data types. Hence, we resort to storing them as strings. 
        We require our representation to preserve ordering and inequalities, while being string-comparable.

        By default, MongoDB does not normalize decimal128 variables, but we do. The uniform representation 
        assumes that the first digit of the mantissa is 0 followed by the decimal dot. 
        E.g. 1.23 becomes 0.123 * 10^(1). In our representation, since "0." is always assumed, we do not need to store it.

        To avoid dealing with signs in the exponent, we take its biased form. Thus, values for the exponent range from 0 to 12321. 

        Negative numbers need to appear before positives, so we prepend all negatives with "-". 
        Positives do not require a sign. For negative numbers, in terms of absolute value, 
        the order is reversed (compared to positives). In other words, greater values for 
        exponents denote smaller numbers; same applies for the mantissa.

        So, for the exponent, if negative, we reverse the order by subtracting from 12321. 
        Thus exponent "-6144" is the first exponent if the number is negative. For the mantissa, 
        if negative, we subtract it from 1.0. "-0.123" becomes "0.877". 

        Given the above, Decimal128.MinValue ("9.99鈥?99E-6144") becomes 000000000鈥?0001 (first 5 zeros are the exponent).  
        Zero must appear, after all negatives and before all positives. We store it as "000000". Negative zero is the same. 
        Special values "NaN", "-infinity", "(+)infinity" become "!nan", "*infinity", "infinity" in order to comply with MongoDB's ordering.

        Some examples:
        Human readable form                          Our normalized string representation
        -9.999999999999999999999999999999999E+6144   -000000000000000000000000000000000000001   MinValue
        -9.999999999999999999999999999999998E+6144   -000000000000000000000000000000000000002
        -0.00000000000000000000000000000002E-6144    -123208
        -0.00000000000000000000000000000001E-6144    -123209 Minus min absolute value
        -0                                           000000
        0                                            000000
        0.00000000000000000000000000000001E-6144     000011  Min absolute value
        0.00000000000000000000000000000002E-6144     000012
        1                                            061771
        2                                            061772
        100                                          061791
        9.999999999999999999999999999999998E+6144    123219999999999999999999999999999999998
        9.999999999999999999999999999999999E+6144    123219999999999999999999999999999999999    MaxValue
        */

        const Decimal128consts = {
            zero: "000000",
            zeroPad: "0000000000000000000000000000000000000000000000",
            ninePad: "9999999999999999999999999999999999999999999999",
            exponentBiasPositive: 6176,
            exponentBiasNegative: 6145,
            exponentLowestAbsoluteValue: 6144,
            infinity: "infinity",
            NanNorm: "!nan",
            NaN: "NaN",
            minusInfinity: "-infinity",
            minusInfinityNorm: "*infinity",
        };

        // Given a string in our normalized string representation, 
        // break it down into isPositive (bool), exponent, mantissa.
        function decomposeString(input) {
            var isPositive = (input[0] !== '-');
            var exponent;
            var mantissa;

            if (isPositive) {
                exponent = parseInt(input.substring(0, 5)) - Decimal128consts.exponentBiasPositive;
                mantissa = input.substring(5, 40);
            }
            else {
                exponent = Decimal128consts.exponentBiasNegative - parseInt(input.substring(1, 6));
                mantissa = input.substring(6, 41);
            }
            return [isPositive, exponent, mantissa];
        }

        // Add two positives (school-style).
        // Returns a 3ple: exponentOffsetValue (how much the common exponent should change),
        // the resulting mantissa string, and if the number is negative.
        function addPositives(firstMantissa, secondMantissa) {
            if (firstMantissa.length != secondMantissa.length)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.unequalLength, '1'));

            var carryBit = 0;
            var sum;
            var result = [];
            for (var i = firstMantissa.length - 1; i >= 0; i--) {
                sum = firstMantissa.charCodeAt(i) % 48 + secondMantissa.charCodeAt(i) % 48 + carryBit;
                carryBit = (sum >= 10) ? 1 : 0;
                result.unshift(sum % 10);
            }

            if (carryBit == 1) {
                result.unshift(1);
            }
            return [carryBit, result.join(""), false];
        }

        // If a > 0, and b <0, it holds that a + b = a + 10 - |b| = 10 + (a-|b|) = c 
        // Hence, if c >= 10 => (a-|b|) >= 0
        // else, if c < 10 => (a-|b|) < 0 
        // Returns a 3ple: exponentOffsetValue (how much the common exponent should change),
        // the resulting mantissa string, and if the number is negative.
        function addPositiveNegative(firstMantissa, secondMantissa, commonExponent) {
            if (firstMantissa.length != secondMantissa.length)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.unequalLength, '2'));

            var carryBit = 0;
            var sum;
            var result = [];
            for (var i = firstMantissa.length - 1; i >= 0; i--) {
                sum = firstMantissa.charCodeAt(i) % 48 + secondMantissa.charCodeAt(i) % 48 + carryBit;
                carryBit = (sum >= 10) ? 1 : 0;
                result.unshift(sum % 10);
            }

            // count number of 0s in beginning, to be trimmed.
            var zeros = 0;
            var nines = 0;
            if (carryBit > 0 && result[0] == 0) {
                for (var i = 0; i < result.length; i++) {
                    if (result[i] !== 0)
                        break;
                    zeros++;
                }
            }
            else if (result[0] == 9) { //case 1.23 + (-1.24 =(873)) = -0.01 = 999
                for (var i = 0; i < result.length; i++) {
                    if (result[i] !== 9)
                        break;
                    nines++;
                }
            }

            var allowedOffsetChange = commonExponent + Decimal128consts.exponentLowestAbsoluteValue;

            var resultIsNegative = (carryBit == 0);
            var exponentOffset = 0;
            var actualOffset;
            if (!resultIsNegative) {
                if (zeros > 0) {
                    actualOffset = Math.min(zeros, allowedOffsetChange);
                    result = result.slice(actualOffset, result.length);
                    exponentOffset = -actualOffset;
                }
            }
            else {
                if (nines > 0) {
                    actualOffset = Math.max(0, Math.min(nines, allowedOffsetChange, result.length - 1));
                    result = result.slice(actualOffset, result.length);
                    exponentOffset = -actualOffset;
                }
            }

            //Final carryBit value determines result (if negative)
            return [exponentOffset, result.join(""), (carryBit == 0)];
        }

        // Add the mantissas of two negative numbers (in our representation).
        // Returns a 3ple: exponentOffsetValue (how much the common exponent should change),
        // the resulting mantissa string, and if the number is negative.
        function addNegatives(firstMantissa, secondMantissa) {
            if (firstMantissa.length != secondMantissa.length)
                throw new Error(ErrorCodes.BadRequest, sprintf(errorMessages.unequalLength, '3'));

            var carryBit = 0;
            var sum;
            var result = [];
            var exponentOffset = 0;
            for (var i = firstMantissa.length - 1; i >= 0; i--) {
                sum = firstMantissa.charCodeAt(i) % 48 + secondMantissa.charCodeAt(i) % 48 + carryBit;
                carryBit = (sum >= 10) ? 1 : 0;
                result.unshift(sum % 10);
            }

            if (carryBit == 0) {
                result.unshift(8);
                exponentOffset = 1;
            }
            else //carryBit == 1
            {
                var isZero = true;
                //check if result is 0
                for (i = 0; i < result.length; i++) {
                    if (result[i] !== 0 && result[i] !== '\0') {
                        isZero = false;
                        break;
                    }
                }
                if (isZero) {
                    result.unshift(9);
                    exponentOffset += 1;
                }
            }

            return [exponentOffset, result.join(""), true];
        }

        // Given the mantissa of a negative number (in our form)
        // extract the "proper" mantissa value
        function invertNegativeNumber(mantissa) {

            var carryBit = 0;
            var sum;
            var result = [];
            for (var i = mantissa.length - 1; i >= 0; i--) {
                var temp = 58 - mantissa.charCodeAt(i) - carryBit;
                carryBit = 1;
                result.unshift(temp % 10);
            }
            return result.join("");
        }

        // Given an operation on our normalized string representation of Decimal128 variables,
        // we need to store a value to the $s field which is what the customer will get back.
        function returnDolarSRepresentation(originalStringRepr) {
            if (originalStringRepr == Decimal128consts.zero)
                return "0";
            if (originalStringRepr == Decimal128consts.NanNorm)
                return Decimal128consts.NaN;
            if (originalStringRepr == Decimal128consts.minusInfinityNorm)
                return Decimal128consts.minusInfinity;
            if (originalStringRepr == Decimal128consts.infinity)
                return originalStringRepr;

            var [isPositive, exponent, mantissa] = decomposeString(originalStringRepr);

            var finalMantissa, finalRepr, finalExponent;
            if (isPositive) {
                finalMantissa = mantissa[0] + "." + mantissa.substr(1, mantissa.length);
                finalRepr = finalMantissa;
                finalExponent = exponent - 1;
                if (finalExponent > 0)
                    finalRepr += "E+" + finalExponent;
                else if (finalExponent < 0)
                    finalRepr += "E" + finalExponent;
            }
            else {
                mantissa = invertNegativeNumber(mantissa);
                finalMantissa = "-" + mantissa[0] + "." + mantissa.substr(1, mantissa.length);
                finalRepr = finalMantissa;
                finalExponent = exponent - 1;
                if (finalExponent > 0)
                    finalRepr += "E+" + finalExponent;
                else if (finalExponent < 0)
                    finalRepr += "E" + finalExponent;
            }

            return finalRepr;
        }

        // This function takes as input two Decimal128 in 
        // our "normalized string representation" and returns
        // the result in the same form.
        function addNormalizedStringRepresentations(first, second) {

            // Quick cases when dealing with zeros, +-infinity, Nan
            if (first == Decimal128consts.zero)
                return second;
            if (second == Decimal128consts.zero)
                return first;
            if (first == Decimal128consts.NanNorm || second == Decimal128consts.NanNorm)
                return Decimal128consts.NanNorm;                                
            if (first.indexOf(Decimal128consts.infinity) !== -1)
                return first;
            if (second.indexOf(Decimal128consts.infinity) !== -1)
                return second;

            // keep second to be the negative
            if (first < Decimal128consts.zero && second > Decimal128consts.zero) {
                var tmp = second;
                second = first;
                first = tmp;
            }

            // decompose strings
            var [firstIsPositive, firstExponent, firstMantissa] = decomposeString(first);
            var [secondIsPositive, secondExponent, secondMantissa] = decomposeString(second);

            var exponentDiff = firstExponent - secondExponent;
            if (Math.abs(exponentDiff) > 34) {
                return null; // Decimal128 does not support more than 34 digits of precision
            }

            // Bring to same exponent and length
            var commonExponent;
            if (exponentDiff > 0) {
                //first is orders of magnitude larger than second. Second should be prepended with 0s.
                if (secondIsPositive) {
                    secondMantissa = Decimal128consts.zeroPad.substr(0, exponentDiff) + secondMantissa;
                } else {
                    secondMantissa = Decimal128consts.ninePad.substr(0, exponentDiff) + secondMantissa;
                }
                firstMantissa = (firstMantissa + Decimal128consts.zeroPad).substr(0, secondMantissa.length);
                commonExponent = firstExponent;
            }
            else if (exponentDiff < 0) {
                if (firstIsPositive) {
                    firstMantissa = Decimal128consts.zeroPad.substr(0, -exponentDiff) + firstMantissa;
                } else {
                    firstMantissa = Decimal128consts.ninePad.substr(0, -exponentDiff) + firstMantissa;
                }
                secondMantissa = (secondMantissa + Decimal128consts.zeroPad).substr(0, firstMantissa.length);
                commonExponent = secondExponent;
            }
            else { //same exponent, adjust lengths
                if (firstMantissa.length < secondMantissa.length)
                    firstMantissa = (firstMantissa + Decimal128consts.zeroPad).substr(0, secondMantissa.length);
                else if (secondMantissa.length < firstMantissa.length)
                    secondMantissa = (secondMantissa + Decimal128consts.zeroPad).substr(0, firstMantissa.length);
                commonExponent = firstExponent;
            }

            var res, resultRepr, isNegative;
            if (firstIsPositive && secondIsPositive) {
                res = addPositives(firstMantissa, secondMantissa);
            }
            else if (firstIsPositive && !secondIsPositive) {
                res = addPositiveNegative(firstMantissa, secondMantissa, commonExponent);
            }
            else { //both negatives
                res = addNegatives(firstMantissa, secondMantissa);
            }

            commonExponent += res[0];
            resultRepr = res[1].replace(/0*$/g, ''); //.trimRight('0'); <--does not work as expected
            isNegative = res[2];
            if (resultRepr == "") {
                return Decimal128consts.zero;
            }

            //Form final string (also check final values of exponent, length of mantissa).
            var finalStringRepr, normExponent;
            if (isNegative) {
                normExponent = Decimal128consts.exponentBiasNegative - commonExponent;
                if (normExponent < 0 || normExponent > 12321)
                    return null;
                normExponent = (Decimal128consts.zeroPad + normExponent).slice(-5); //left pad 0s
                finalStringRepr = "-" + normExponent + resultRepr;
            }
            else {
                normExponent = commonExponent + Decimal128consts.exponentBiasPositive;
                if (normExponent < 0 || normExponent > 12321)
                    return false;
                normExponent = (Decimal128consts.zeroPad + normExponent).slice(-5); //left pad 0s
                finalStringRepr = normExponent + resultRepr;
            }
            
            if (resultRepr.length > 34)
                return null;

            return finalStringRepr;
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
                    var accepted = collection.queryDocuments(collection.getSelfLink(), query, requestOptions, (error, resources, responseOptions) => {
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
                                resolve({ continuation: responseOptions.continuation, items: flattened });
                            }
                        }
                    });
                    if (!accepted) {
                        reject(error);
                    }
                };

                queryNext();
            };
            return new Promise(executor);
        }

        function replaceDocumentAsync(doc, options) {
            var collection = getContext().getCollection();
            var executor = (resolve, reject) => {
                var accepted = collection.replaceDocument(doc._self, doc, options, (error, resources) => {
                    if (error) {
                        reject(error);
                    }
                    else {
                        resolve(resources);
                    }
                });
                if (!accepted) {
                    reject(error);
                }
            };
            return new Promise(executor);
        }
    }
    setupSystemFunctions();
}
//---------------------------------------------------------------------------------------------------
