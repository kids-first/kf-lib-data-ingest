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
