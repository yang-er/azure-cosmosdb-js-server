//---------------------------------------------------------------------------------------------------
//
// In this script, keep only UDFs.
//
//---------------------------------------------------------------------------------------------------
// Code should run in strict mode wherever possible.
"use strict";
//---------------------------------------------------------------------------------------------------
if (typeof __ == "undefined") {
    var __ = new Object();  // It's OK to define inside if, as vars have function (in this case global) scope.
}

{
    // Use 'let' inside a block so that everything defined here is not visibile to user script.
    let setupSystemFunctions = function () {
        if (typeof __.sys == "undefined") __.sys = new Object();
        setupSys.call(__.sys);
        //---------------------------------------------------------------------------------------------------
        function setupSys() {
            // Use defineProperty/freeze to prevent the user from changing __.sys.*. echo is not enumerable, the others are.
            // Note: set enumerable to true, if you want the function to be enumerated as properties of the __ object.
            Object.defineProperty(this, "regex_match", { enumerable: false, configurable: false, writable: false, value: regex_match });
            Object.freeze(this);
        }
        //---------------------------------------------------------------------------------------------------
        /**
          * System function: regex_match
          * Checks if a string matches a Javascript regex
          * @param {string/array} candidate  - The candidate object to be matched against a regex.
          * @param {string} regexStr         - A Javascript regex represented as a string (empty regex-pattern should match all string values).
          * @param {string} [regexOptions]   - A string represents the Javascript regex flags ( allowed flags are i, g, m ).
          * @param {string} [searchWithinArrays]  - A boolean indicating if the process should search within array values.
         */
        function regex_match(candidate, regexStr, regexOptions, searchWithinArrays) {
            var errorMessages = {
                invalidFunctionCall: 'The regex_match function requires at least two arguments.',
                invalidRegex: 'Invalid argument. The regular expression specified by the \'regexStr\' parameter is invalid.',
                invalidRegexOptions: 'Invalid argument. Only JavaScript regex flags are supported (i, g, m, u)',
                invalidSearchWithinArrays: 'Invalid argument. Only boolean variables are supported for searchWithinArrays flag.',
            };

            if (arguments.length < 2) {
                throw new Error(errorMessages.invalidFunctionCall);
            }

            // Regex pattern can be an empty string. 
            // We should get match it to all documents where the property is a string (including empty string),
            // and arrays that contain one or more strings (including empty string).
            if (typeof (regexStr) !== "string" || regexStr === null) {
                return false;
            }

            // Empty candidate string is a valid input; we should be able to match empty-string regex-patterns to empty strings
            if ((typeof (candidate) !== "string" && !Array.isArray(candidate)) || candidate === null) {
                return false;
            }

            if (regexOptions) {
                if (typeof (regexOptions) !== "string") {
                    throw new Error(errorMessages.invalidRegexOptions);
                }

                var regexOptionsValidation = regexOptions.search(/^[igmu]{0,4}$/);
                if (regexOptionsValidation === -1) {
                    throw new Error(errorMessages.invalidRegexOptions);
                }
            }

            if (searchWithinArrays) {
                if (typeof (searchWithinArrays) !== "boolean") {
                    throw new Error(errorMessages.invalidSearchWithinArrays);
                }
            }

            var regex = null;
            try {
                regex = new RegExp(regexStr, regexOptions);
            } catch (err) {
                throw new Error(errorMessages.invalidRegex + ": " + err.message);
            }

            // Case: candidate is string
            if (typeof (candidate) == "string") {
                return candidate.search(regex) >= 0;
            }

            // Case: candidate is array
            // Search within arrays by default, unless explicitly indicated.
            if (searchWithinArrays === false) {
                    return false;
            }

            var arrayLength = candidate.length;
            for (var i = 0; i < arrayLength; i++) {
                // old schema (array contains numbers/strings/objects)
                if (candidate[i] === null) {
                    continue;
                }
                else if (typeof (candidate[i]) == "string") {
                    if (candidate[i].search(regex) >= 0) {
                        return true;
                    }
                }
                // Bson Schema (array contains {"$t": <type>, "$v":<value>} dictionaries)
                else if (typeof (candidate[i]) == "object" &&  "$t" in candidate[i] && candidate[i]["$t"] == 2) {
                    var value = candidate[i]["$v"];
                    if (value.search(regex) >= 0) {
                        return true;
                    }
                }
            }
            return false;

        }
        //---------------------------------------------------------------------------------------------------
    }
    setupSystemFunctions();
}
//---------------------------------------------------------------------------------------------------
