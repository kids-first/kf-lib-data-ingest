#  Change History

## Release 0.11.0

### Summary

- Emojis: 📝 x1, 🚧 x1, 💡 x1
- Categories: Documentation x2, Other Changes x1

### New features and changes

- [#451](https://github.com/kids-first/kf-lib-data-ingest/pull/451) - 📝 Add psycopg2 to the install instructions - [3b64a076](https://github.com/kids-first/kf-lib-data-ingest/commit/3b64a076a2211286890a0195222aa9d826254ba2) by [fiendish](https://github.com/fiendish)
- [#447](https://github.com/kids-first/kf-lib-data-ingest/pull/447) - 🚧 Add sample procurement - [fb55890f](https://github.com/kids-first/kf-lib-data-ingest/commit/fb55890f469b4d755e2862e132538fcff7d30de1) by [liberaliscomputing](https://github.com/liberaliscomputing)
- [#445](https://github.com/kids-first/kf-lib-data-ingest/pull/445) - 💡 Explain why AUTOCOMMIT is invoked - [127be01e](https://github.com/kids-first/kf-lib-data-ingest/commit/127be01e82994bb210246a33290176799c49b814) by [fiendish](https://github.com/fiendish)

# Kids First Lib Data Ingest Release 0.10.0

## ⚠️ Important - README!

This release has a very important change (a part of #433) which fixes the issue where
Metabase would fail to delete databases containing schema names with slashes. This
would leave zombie database entries in Metabase that had no way to be deleted.

Please STOP USING previous releases as they will cause creation of the zombie databases!

## Features

### Summary

Feature Emojis: ♻️ x1 ✏️ x2 ✏️Add x1 ✏️Refactored x1 🐛 x2 💬 x1
Feature Categories: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x2 [other](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/other) x5 [refactor](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/refactor) x1

### New features and changes

- (#440) ✏️ Fix typo in ingest success log message - @chris-s-friedman
- (#438) 💬 Make all constants into strings for sanity - @fiendish
- (#436) ✏️Refactored docstrings to all be in RST format - @gsantia
- (#435) ✏️Add quickstart - @gsantia
- (#434) 🐛 Update outdated tutorial sections and re-order them - @znatty22
- (#433) 🐛 Don't wipe whole db for each package - @fiendish
- (#429) ✏️ Fix bad link in doc - @fiendish
- (#427) ♻️ Deduplicate discovery - @fiendish

# Kids First Lib Data Ingest Release 0.9.0

## Features

- Warehouse ingest stage outputs
- Speed up accounting by only counting values for columns (sources) and not
pairs of columns (links)

### Summary

Feature Emojis: Create x1 Update x1 ♻️ x2 ⚡️🚧 x1 ✅ x1 ✨ x2 🐛 x3 🐶 x1 📝 x3 🔥 x1 🚧 x1
Feature Categories: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x3 [documentation](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/documentation) x3 [feature](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/feature) x2 [other](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/other) x7 [refactor](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/refactor) x2

### New features and changes

- (#425) 📝 Documentation for warehousing - @fiendish
- (#424) Create clean.sh - @fiendish
- (#423) ✨ Add enrollment age concept - @chris-s-friedman
- (#422) 🐛 Unbreak outer_merge with multiple join cols - @fiendish
- (#420) 📝 Make template transform_module validate - @fiendish
- (#419) 🐶 Add Species - @liberaliscomputing
- (#418) ⚡️🚧 Only do accounting on concept instances  - @znatty22
- (#417) 🐛 Fix some missing/broken bits of warehousing - @fiendish
- (#416) ✨ Add methods for sending data to a db - @fiendish
- (#415) ♻️ Refactor message packing out of transform - @fiendish
- (#411) 🔥 Burn down the graph code because we'll never use it as is. - @fiendish
- (#410) 📝 Make doc for custom reads more prominent - @fiendish
- (#407) Update documentation - @fiendish
- (#406) 🚧 Fix read_df Errors - @liberaliscomputing
- (#404) ✅ Keep FileRetriever().get() safe - @fiendish
- (#403) ♻️ Move reader selector out of ExtractStage - @fiendish
- (#400) 🐛 It was always supposed to be UL not ML - @fiendish


# Kids First Lib Data Ingest Release 0.8.0

## Features

### Summary

Feature Emojis: ✨ x1 ⬆️ x1 🎨 x1 🐛 x2 🔇 x1 🔊 x1 🔧 x1
Feature Categories: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x2 [feature](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/feature) x1 [other](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/other) x5

### New features and changes

- (#395) 🐛 Keep extracted UKs in comparison - @fiendish
- (#393) 🔇 Only show comparison mismatches - @fiendish
- (#391) ⬆️ Upgrade deps - @fiendish
- (#390) 🔊 Add bad value and type to type assert output - @fiendish
- (#389) 🎨 Format with black - @fiendish
- (#388) 🔧 Add visible to family_relationship in kf target api - @znatty22
- (#385) 🐛 Update entity relationship external_id on mismatch - @fiendish
- (#384) ✨ Add gf source file to concept schema - @znatty22

# Kids First Lib Data Ingest Release 0.7.0

## Features

### Summary

Feature Emojis: ♻️ x2 ♻️⚡️ x1 ♻️💥 x1 ✨ x2 🐛 x5 💡 x1 💬 x3 📝 x1
Feature Categories: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x5 [documentation](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/documentation) x1 [feature](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/feature) x2 [other](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/other) x6 [refactor](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/refactor) x2

### New features and changes

- (#383) 💡 Cols is optional, delimiters is not - @fiendish
- (#381) 🐛 Replace obsolete etag concept in boyd test - @fiendish
- (#380) 💬 Print column lists nicer - @fiendish
- (#379) ♻️💥 Change "load" to "read" - @fiendish
- (#377) ♻️ Factor out pandas read file functions - @fiendish
- (#375) 💬 Nicer op messages - @fiendish
- (#374) ✨ FileRetriever static auth configs - @fiendish
- (#372) ✨ Let value_map take a regex pattern also - @fiendish
- (#369) 📝 Docs expansion - @fiendish
- (#365) ♻️⚡️ Don't validate counts from prior run - @fiendish
- (#364) ♻️ Move request logging into session object - @fiendish
- (#363) 🐛 Indexd mandates a specific hash key format - @fiendish
- (#362) 🐛 Add GF ACLs to concepts and target_schema - @fiendish
- (#359) 🐛 Make sure the interim output gets written - @fiendish
- (#358) 💬 Add new datatype constants - @fiendish
- (#357) 🐛 Use relative path as extract output key instead of full path - @fiendish

# Kids First Lib Data Ingest Release 0.6.0

## Features

### Summary

Feature Emojis: ✨ x4 🐛 x1 👷 x2
Feature Categories: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x1 [devops](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/devops) x2 [feature](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/feature) x4

### New features and changes

- (#352) 🐛 Exit on thread exception - @fiendish
- (#350) ✨ Add type mapping to target api config - @znatty22
- (#349) 👷 Updates Constants - @liberaliscomputing
- (#348) 👷 Blanket Ignore All Dotfiles - @liberaliscomputing
- (#347) ✨ Add ETag to constants - @znatty22
- (#344) ✨ Optional multithreaded loading - @fiendish
- (#343) ✨ Error on inconsistent merge inputs - @fiendish

# Kids First Lib Data Ingest Release 0.5.0

## Features

### Summary

Feature Emojis: ♻️ x3 ✨ x6 🐛 x6 🐛🔧 x1 🚸 x1
Feature Categories: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x6 [feature](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/feature) x6 [other](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/other) x2 [refactor](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/refactor) x3

### New features and changes

- (#338) 🐛 Remove errant layer from basic auth test config - @fiendish
- (#333) 🐛 Fix merge col validation bug in merge_wo_duplicates - @znatty22
- (#330) ♻️ Refactor BIOSPECIMEN concept -> BIOSPECIMEN, BIOSPECIMEN_GROUP - @znatty22
- (#329) 🐛 Fix invalid use of pytest context managers - @fiendish
- (#328) 🐛 Fix diagnosis event age concept - @fiendish
- (#327) ✨ Support resuming load from a given target ID or prefix - @fiendish
- (#325) 🚸 Add friendly merge column alert - @fiendish
- (#324) ✨ Add family relationship to concept schema and kids first api config - @znatty22
- (#322) ♻️ Transform function outputs dict of DataFrames - @znatty22
- (#319) ♻️ Refactor links in target api cfg  - @znatty22
- (#316) 🐛 Load objects instead of object-like strings - @fiendish
- (#315) ✨ Add grouping of Splits - @fiendish
- (#314) 🐛🔧 Fix/update genomic file std and target concept configuration - @znatty22
- (#311) ✨ Use supplied target service ids if provided - @znatty22
- (#309) 🐛 Load entities in order defined by target api cfg - @znatty22
- (#305) ✨ Write transform stage output to TSV - @znatty22
- (#299) ✨ Run subsets of ingest pipeline - @znatty22

# Kids First Lib Data Ingest Release 0.4.0

## Features

### Summary

Feature Emojis: 🐛 x2 💬 x1 🔊 x1 🚚 x1
Feature Categories: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x2 [other](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/other) x3

### New features and changes

- (#303) 🐛 Fix numeric_to_str - vals that equate to 0 should return '0' - @znatty22
- (#297) 💬 Don't start concept strings with 'CONCEPT|' - @fiendish
- (#293) 🔊 Log release version and commit sha - @znatty22
- (#292) 🚚 Mv network funcs into network utils - @znatty22
- (#290) 🐛 Don't access attributes on strings - @fiendish

# Kids First Lib Data Ingest Release 0.3.0

## Features

### Summary

Feature Emojis: ♻️ x2 ✨ x1 🐛 x3 🚸 x1
Feature Labels: [bug](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x3 [feature](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/feature) x1 [other](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/other) x1 [refactor](http://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/refactor) x2

### New features and changes

- (#285) ♻️ Update cat study - @znatty22
- (#284) ♻️ move timestamp() to common and refactor setup_logger - @fiendish
- (#283) 🚸 Nicer handling of missing(stubbed) data - @fiendish
- (#279) 🐛 Fix user defined test bugs - @znatty22
- (#274) 🐛 Clean up the transform output df - @fiendish
- (#273) 🐛 click.Choice needs a list, not an iterator - @fiendish
- (#254) ✨ Post transform accounting - @fiendish

# Kids First Lib Data Ingest Release 0.2.0

## Features

### Summary

Feature Emojis: 🐛x2 🔒x2 🔧x1 ✨x1 🔥x1
Feature Labels: [refactor](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/refactor) x2 [devops](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/devops) x2 [documentation](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/documentation) x1 [bug](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x1

### New features and changes

- (#267) 🔧 Update dev settings - pass token in req header - @znatty22
- (#262) ✨ Add app settings sect to docs - @znatty22
- (#266) 🐛 Fix logging with fetching files - @znatty22
- (#265) 🐛 fix extended-attr filename parsing bugs - @fiendish
- (#257) 🔥 Rm docker self clean from entrypoint - @znatty22
- (#258) 🔒 No auth environment logging - @fiendish
- (#253) 🔒 lock down merges - @fiendish

# Kids First Lib Data Ingest Release 0.1.0

## Features

### Summary

Feature Emojis: ✨x28 🐛x18 ♻️x18 x11 📝x5 👷x4 💄x3 🐳x2 🔊x2 ⬆️x2 🚚x1 ✅x1 ✨🐛x1 🔥x1 🔇x1 🖼x1  🚨x1 🙈x1 ⚡️x1 ♻️✨x1 🔧x1 📦x1 🚸x1 🚧x1
Feature Labels: [feature](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/feature) x36 [refactor](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/refactor) x20 [bug](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/bug) x18 [documentation](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/documentation) x12 [devops](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/devops) x9 [tests](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/tests) x4 [utilities](https://api.github.com/repos/kids-first/kf-lib-data-ingest/labels/utilities) x1

### New features and changes

- (#252) 🐳 Docker container clean up after ingest ends - @znatty22
- (#250) 🐛 expected_counts not required - @fiendish
- (#251) 🐛 lookup tables weren't linked in the target api - @fiendish
- (#248) 🚚 dataset_ingest_config -> ingest_package_config - @fiendish
- (#233) ♻️ dataset_ingest_config to python module - @fiendish
- (#246) ♻️ small change to logging - @fiendish
- (#244) 🐛 Update broken link in extract tutorial - @znatty22
- (#239) 📝 Update tutorial - test ingest package, transform stg - @znatty22
- (#242) 🐛 Add missing pytest and re-org dev-requirements - @znatty22
- (#225) 👷 Add Jenkinsfile - @znatty22
- (#189) ✨ Add Test CLI cmd - @znatty22
- (#231) ✅ User defined data validation tests - @znatty22
- (#229) ✨ Load stage (part 1 of several) - @fiendish
- (#238) 🐛 Fix bug with optional components in unique keys - @znatty22
- (#237) ♻️ Use term "ingest package" consistently in docs site - @znatty22
- (#235) 📝  Clean up README - @znatty22
- (#214) 🐳 Dockerize ingest app - @znatty22
- (#230) ♻️ Make template ingest pkg usable - @znatty22
- (#228) ♻️ Small refactor stage checks - @znatty22
- (#227) ✨ Remove need to manually forward config elements - @fiendish
- (#210) ✨ App settings - @znatty22
- (#224) ♻️ Make target api file fields, class var names consistent - @znatty22
- (#223) 🐛 no fail on no expectations - @fiendish
- (#222) ✨🐛 visibility attribute mirror - @fiendish
- (#221) 🐛 Fix rogue nulls - @znatty22
- (#211) ♻️ Separate stage tally format from checks - @fiendish
- (#216) 🔥 go away, test_graph - @fiendish
- (#199) ✨ Add auth to file retriever - @znatty22
- (#212) 🔊 Log retry messages better - @fiendish
- (#203) 🐛 Retrying is free. Don't give up so easily. - @fiendish
- (#208) 🐛 Use safe YAML loader - @znatty22
- (#206) 🐛 Rm ingest test_study from build.sh - @znatty22
- (#200) ♻️ reshape the stage queue - @fiendish
- (#198) ♻️ move target uid lookup out of transform into load - @fiendish
- (#195) ♻️ Refactor guided transform stage - @znatty22
- (#197) 🐛 The dataservice throws 500 if gen3 does - @fiendish
- (#144) ✨ Post-extract accounting - @fiendish
- (#190) 🔇 Don't emit so much null conversion spam - @fiendish
- (#184) ♻️ Preserve original file's ext in fetched temp file - @znatty22
- (#180)  ♻️ merge transform/common into TransformStage - @fiendish
- (#182)  📦 stop complaining about test_graph being changed - @fiendish
- (#181) 🖼 Update logo - @dankolbman
- (#153) ✨ Source ID to Target ID Translation - @znatty22
- (#174) 🐛 🔊 Fix null processing and better logging - @znatty22
- (#172) ✨ Make ingest-pipeline.png clickable - @znatty22
- (#168) 🐛 boyd test config has an inverted check on hidden families - @fiendish
- (#152) ✨ Kids First Null Processing - @znatty22
- (#165) 👷Run ingest in CI to verify build worked - @znatty22
- (#164) ✨ Small Logging Improvements - @znatty22
- (#163)  ✅ catch if load_func doesn't return a df - @fiendish
- (#162) ✨ Finish guided transform - @znatty22
- (#157) 🐛 Add requests package to requirements - @znatty22
- (#142) ✨ Unique Key Refactor and Other Fixes - @znatty22
- (#155) 🚨 Pandas's SettingWithCopyWarning is so annoying - @fiendish
- (#148)  🐛 extract data cleaning (bug #147) - @fiendish
- (#146) ♻️ refactor _source_file_to_df - @fiendish
- (#143)  🐛 normalize dtypes in extract output to string - @fiendish
- (#109) 📝 how to configure a new study - @fiendish
- (#123) 🐛 Fix for #122. Helpful error instead of unhelpful error on missing file protocol. - @fiendish
- (#119) 👷 Fix CI Scripts - @znatty22
- (#120) 🙈 stop spamming me with sphinx files - @fiendish
- (#117) ⚡️ Use sphinx autobuild - @znatty22
- (#118) 🐛 Fix url for kf sphinx theme req - @znatty22
- (#110) 💄 Update docs to use Kids First Style - @znatty22
- (#93) ✨ support splitting into multiple rows - @fiendish
- (#107) ✨ preserve row index across extract - @fiendish
- (#105) ✨ GuidedTransformer - apply user transform funct - @znatty22
- (#104) ♻️✨ Modify ExtractStage to write out tables - @znatty22
- (#103) ♻️ Refactor stage read write - @znatty22
- (#100) ♻️ Refactor transform stage to use Auto/Guided transformers - @znatty22
- (#102) ✨ Add merge functions to pandas utils - @znatty22
- (#98) ♻️ Minor refactor transform, load - @znatty22
- (#97) ♻️ Cleanup leftover src - @znatty22
- (#96) ⬆️ Upgrade pyyaml to fix vulnerability - @znatty22
- (#57) ✨ Standard model + transformation - @znatty22
- (#90) 🔧 Boyd study changes - @fiendish
- (#91) ⬆️ pep8 has been renamed to pycodestyle - @fiendish
- (#88) 📦 Fix package installation - @znatty22
- (#86)  🐛 continue past failing s3 profiles - @fiendish
- (#84) 🐛 use auth profiles for s3 requests - @fiendish
- (#80)  🐛 extract from the fetched files, not the original paths - @fiendish
- (#79) ✨ serialize/deserialize extract stage output - @fiendish
- (#77)  ✅ more tests for extract output - @fiendish
- (#78)  🔊 log file retrievals + use stage name - @fiendish
- (#81) 🐛 Fix result of find/replace in README html - @znatty22
- (#72) 📝 Initial Sphinx Documentation - @znatty22
- (#75) 💄 Make logo smaller - @znatty22
- (#73) ✨ Extract pipeline - @fiendish
- (#74) 💄 Kids-firstify the repo - @znatty22
- (#51) ✨ Target API Config - @znatty22
- (#67) ✨ pandas utility functions - @fiendish
- (#66)  💬 constants file in its own PR - @fiendish
- (#63) 🚸 show traceback from caller - @fiendish
- (#65)  ⬆️ python 3.6.1 -> 3.7 - @fiendish
- (#64) ✨ ConfigValidationError for config validation errors - @fiendish
- (#60) ♻️ Refactor CLI - @znatty22
- (#61) ♻️ make type check of list values its own function - @fiendish
- (#58) ✨ type checking for object attributes, lists, and calls - @fiendish
- (#47) ✨ Initial concept graph - @znatty22
- (#52) ✨ add safety funcs for e.g. type checks - @fiendish
- (#20) ✨ File getter - @fiendish
- (#45) ✨ Concept schema and tests - @znatty22
- (#21) 🔊 Setup logging - @znatty22
- (#42) 📝 Add basic dev section to README - @znatty22
- (#39) ✨ Add basic CLI - @znatty22
- (#16) 👷 Add initial CI - @znatty22
- (#14) 🚧  Basic skeleton and initial tests - @znatty22