# Changelog

## [1.29.0](https://github.com/momentohq/client-sdk-python/compare/v2.0.0...v1.29.0) (2026-09-01)


### ⚠ BREAKING CHANGES

* requires momento-wire-types >=0.135.0 and protobuf >=7.35.1,<8, and raises the minimum Python to 3.10. Support for protobuf 3 and 4 is removed.

### Features

* **#85:** MVP dictionary API ([#98](https://github.com/momentohq/client-sdk-python/issues/98)) ([c4c6bb1](https://github.com/momentohq/client-sdk-python/commit/c4c6bb187576a0eee102bd921fd3f94ed39015d1))
* 1.0 error and response types for signing key operations ([#274](https://github.com/momentohq/client-sdk-python/issues/274)) ([0f42bd8](https://github.com/momentohq/client-sdk-python/commit/0f42bd8bd3bc98f6165c3de14e8a9c1ce2240675))
* 1.0 release ([#307](https://github.com/momentohq/client-sdk-python/issues/307)) ([f63d0ed](https://github.com/momentohq/client-sdk-python/commit/f63d0edd6c930a7733cf9d74ab1d834b67ed3592))
* Add a credential provider for Momento Local ([#501](https://github.com/momentohq/client-sdk-python/issues/501)) ([9f809c8](https://github.com/momentohq/client-sdk-python/commit/9f809c84bfbf35ce8576da773f419b7df741da20))
* Add a parameterized Simple Cache Client ([#21](https://github.com/momentohq/client-sdk-python/issues/21)) ([ce1c252](https://github.com/momentohq/client-sdk-python/commit/ce1c2527a6922f38b2f489f054c33a22006fb797))
* add asyncio client ([#28](https://github.com/momentohq/client-sdk-python/issues/28)) ([67b0484](https://github.com/momentohq/client-sdk-python/commit/67b048402544ce2a4d5001d3401ffd122d6f784d))
* add auth client and generate_disposable_token api ([#460](https://github.com/momentohq/client-sdk-python/issues/460)) ([693a059](https://github.com/momentohq/client-sdk-python/commit/693a059490b8f922fd756ec66e24165abea27e05))
* Add client-side implementation of exists ([#115](https://github.com/momentohq/client-sdk-python/issues/115)) ([7cf76bd](https://github.com/momentohq/client-sdk-python/commit/7cf76bdfb0d13cf8d030a32d434f15da8f4d2c09))
* Add common response __str__ and __repr__ ([#258](https://github.com/momentohq/client-sdk-python/issues/258)) ([b9e5235](https://github.com/momentohq/client-sdk-python/commit/b9e52359f49bb1be1ea89e34c300a185369cee40))
* add configuration classes ([#178](https://github.com/momentohq/client-sdk-python/issues/178)) ([1500893](https://github.com/momentohq/client-sdk-python/commit/15008938ffed9536c0258b34e0c888e9a7618893))
* Add debug logger ([527cd47](https://github.com/momentohq/client-sdk-python/commit/527cd47e6484ad4a7b5666ba2e37de37ca3af522))
* Add debug logger ([#11](https://github.com/momentohq/client-sdk-python/issues/11)) ([7e2c62b](https://github.com/momentohq/client-sdk-python/commit/7e2c62b4c32d3592a4d2937122d3bb135683a6a6))
* Add delete functionality ([#100](https://github.com/momentohq/client-sdk-python/issues/100)) ([72b6bb7](https://github.com/momentohq/client-sdk-python/commit/72b6bb70c47b17ed027ed205eb6c8a4825a258e2))
* add grpc config options and turn off keepalive for Lambda config ([#441](https://github.com/momentohq/client-sdk-python/issues/441)) ([b129716](https://github.com/momentohq/client-sdk-python/commit/b129716b1758bff5f785925c9e6726db04bcfabc))
* Add ListCaches ([#10](https://github.com/momentohq/client-sdk-python/issues/10)) ([6ded6f8](https://github.com/momentohq/client-sdk-python/commit/6ded6f857505683a40213d1820ff782561438b06))
* add middleware ([#502](https://github.com/momentohq/client-sdk-python/issues/502)) ([1230510](https://github.com/momentohq/client-sdk-python/commit/1230510154681507a8cb6a58a28ffde8038e12e8))
* add mvi delete by filter overload ([#437](https://github.com/momentohq/client-sdk-python/issues/437)) ([2c48119](https://github.com/momentohq/client-sdk-python/commit/2c48119460729e197e5d1f00a3ed8e22bdf08250))
* add mvi search and fetch vectors method ([#399](https://github.com/momentohq/client-sdk-python/issues/399)) ([d397f92](https://github.com/momentohq/client-sdk-python/commit/d397f927b177edbbaf9272b0adb0d9a119d478db))
* add mvi search score threshold ([#397](https://github.com/momentohq/client-sdk-python/issues/397)) ([4c5c6b1](https://github.com/momentohq/client-sdk-python/commit/4c5c6b123fbe75bb76d5b0efd258d3cb8be99eb5))
* add pubsub ([#353](https://github.com/momentohq/client-sdk-python/issues/353)) ([ac7b626](https://github.com/momentohq/client-sdk-python/commit/ac7b626b0cff0e7890ca9d1e7b2c2e39119982b8))
* add signing key control operations back in ([#210](https://github.com/momentohq/client-sdk-python/issues/210)) ([f577224](https://github.com/momentohq/client-sdk-python/commit/f577224d6acedf73e27fbe60c58127a0a5a017d3))
* Add sorted set get score and rank ([#326](https://github.com/momentohq/client-sdk-python/issues/326)) ([163688b](https://github.com/momentohq/client-sdk-python/commit/163688b8dc3dca90dcad1c456c2a0b6af4926fa0))
* Add sorted set put and fetch methods ([#324](https://github.com/momentohq/client-sdk-python/issues/324)) ([5695ae5](https://github.com/momentohq/client-sdk-python/commit/5695ae5f9e71e039e1892e3e7ce8c0c44365f6d1))
* Add sorted set remove and increment ([#327](https://github.com/momentohq/client-sdk-python/issues/327)) ([aaddbcc](https://github.com/momentohq/client-sdk-python/commit/aaddbcc299612233355307d2338f481bd0190578))
* add support for CreateSigningKey, RevokeSigningKey, and ListSigningKeys APIs ([#81](https://github.com/momentohq/client-sdk-python/issues/81)) ([6b3fa09](https://github.com/momentohq/client-sdk-python/commit/6b3fa09b24ccd22c0399bc50262381f4e3709622))
* add support for retrying failed requests ([#124](https://github.com/momentohq/client-sdk-python/issues/124)) ([b65f8fb](https://github.com/momentohq/client-sdk-python/commit/b65f8fb0ab9f7c0f9fb8be068fcd2843690a2298))
* Add support for Timeouts for Gets and Sets ([#50](https://github.com/momentohq/client-sdk-python/issues/50)) ([facf5b0](https://github.com/momentohq/client-sdk-python/commit/facf5b00f8c9254ee1e059706b7ad4b317637212))
* add support for v1 auth tokens ([#319](https://github.com/momentohq/client-sdk-python/issues/319)) ([f945e9f](https://github.com/momentohq/client-sdk-python/commit/f945e9fc3facd80e4fdfb83cbd5d60130e37e37a))
* add support through python 3.11 ([#155](https://github.com/momentohq/client-sdk-python/issues/155)) ([176e75c](https://github.com/momentohq/client-sdk-python/commit/176e75c0a0e2411eb1e3017081e8cf03b7bd669b))
* add support to create pre-signed URLs ([#78](https://github.com/momentohq/client-sdk-python/issues/78)) ([262ac0f](https://github.com/momentohq/client-sdk-python/commit/262ac0fa655eff80f781fc135f578bf81cf487ba))
* Add test matrix for 3.10 ([#58](https://github.com/momentohq/client-sdk-python/issues/58)) ([d2342f3](https://github.com/momentohq/client-sdk-python/commit/d2342f305a86b6169660c1e32b0a2376d8e88ebc))
* add topic grpc config and transport strategy and make sure publish deadline is set ([#496](https://github.com/momentohq/client-sdk-python/issues/496)) ([bfac90d](https://github.com/momentohq/client-sdk-python/commit/bfac90d0c235ef79272ddbb54ad80b3ae7d65c82))
* add traces with open telemetry ([#344](https://github.com/momentohq/client-sdk-python/issues/344)) ([3319e78](https://github.com/momentohq/client-sdk-python/commit/3319e78e167cbefee43ce58326af60566fd2de95))
* add vanilla example for mvi ([#371](https://github.com/momentohq/client-sdk-python/issues/371)) ([3ca90e6](https://github.com/momentohq/client-sdk-python/commit/3ca90e624bb031c616ac36dbf6f85e6cd955d1c5))
* add vector index client ([#358](https://github.com/momentohq/client-sdk-python/issues/358)) ([144569a](https://github.com/momentohq/client-sdk-python/commit/144569a32ab11d1602bebf26fee45660a921d599))
* adds flush api ([#329](https://github.com/momentohq/client-sdk-python/issues/329)) ([25d8df4](https://github.com/momentohq/client-sdk-python/commit/25d8df4d82e2ee38b4b2c533c8f2ed43c2ac056e))
* allow custom root certificates in cache client ([#395](https://github.com/momentohq/client-sdk-python/issues/395)) ([c1a3abf](https://github.com/momentohq/client-sdk-python/commit/c1a3abf858e8cd6eb34a330863ec247ac781e152))
* allow custom root certificates in vector index client config ([#394](https://github.com/momentohq/client-sdk-python/issues/394)) ([3a2c7f0](https://github.com/momentohq/client-sdk-python/commit/3a2c7f0df1e38fbce3d007d60d0ca18a731df1a3))
* Base implementation for Cache Operations ([#2](https://github.com/momentohq/client-sdk-python/issues/2)) ([39bbe5b](https://github.com/momentohq/client-sdk-python/commit/39bbe5b7a9d1c90de348774c58dd7fcbda368481))
* bug fix - sorted_set_increment should be named sorted_set_increment_score ([#347](https://github.com/momentohq/client-sdk-python/issues/347)) ([fc735a8](https://github.com/momentohq/client-sdk-python/commit/fc735a8ae224b16f0f9acb7bf16a95f38e3317e4))
* Complete list ops ([#230](https://github.com/momentohq/client-sdk-python/issues/230)) ([207fea2](https://github.com/momentohq/client-sdk-python/commit/207fea2e563f5579c710f113a1f9bde65c6fb546))
* dictionary implementation ([#237](https://github.com/momentohq/client-sdk-python/issues/237)) ([7000d22](https://github.com/momentohq/client-sdk-python/commit/7000d221611d131937a0450bf70654fbef205bb6))
* dictionary multi-get, unary set, and TTL. ([#110](https://github.com/momentohq/client-sdk-python/issues/110)) ([c0cc070](https://github.com/momentohq/client-sdk-python/commit/c0cc07033363ff3f371fe37f4b629d304c1226b2))
* dictionary stub implementation ([#229](https://github.com/momentohq/client-sdk-python/issues/229)) ([b2ebea6](https://github.com/momentohq/client-sdk-python/commit/b2ebea6ee209c351a777e325c383026b208b3bf4))
* Disambiguate wiretypes from SDK types ([#48](https://github.com/momentohq/client-sdk-python/issues/48)) ([c23bbda](https://github.com/momentohq/client-sdk-python/commit/c23bbdaec1b084f80a041e3214607bfcb772c461))
* Enforce keyword args for optional arguments. ([#260](https://github.com/momentohq/client-sdk-python/issues/260)) ([61a28a6](https://github.com/momentohq/client-sdk-python/commit/61a28a61e802b869514ebc9e417a2d39520d1618))
* error and response types ([#179](https://github.com/momentohq/client-sdk-python/issues/179)) ([bd4960f](https://github.com/momentohq/client-sdk-python/commit/bd4960fe63a2f2843dd6ed26a48bb08e57eaeee0))
* export types to be PEP561 compliant ([#206](https://github.com/momentohq/client-sdk-python/issues/206)) ([8263bac](https://github.com/momentohq/client-sdk-python/commit/8263bacf791154ae4ce82bb0811eab1f2a1b3db0))
* First collections methods. ([#205](https://github.com/momentohq/client-sdk-python/issues/205)) ([0736132](https://github.com/momentohq/client-sdk-python/commit/0736132af95eeea73d46780fcb9f3d1c8f1dcd0f))
* implement get item batch and variant ([#417](https://github.com/momentohq/client-sdk-python/issues/417)) ([a28a177](https://github.com/momentohq/client-sdk-python/commit/a28a177aa1d1162d241e405e12ec563e139e2953))
* Improve validation errors. ([#232](https://github.com/momentohq/client-sdk-python/issues/232)) ([0caa89c](https://github.com/momentohq/client-sdk-python/commit/0caa89c81d26b6e24ffede250196d7d82620d1b0))
* increment() ([#245](https://github.com/momentohq/client-sdk-python/issues/245)) ([78ba6cc](https://github.com/momentohq/client-sdk-python/commit/78ba6cc4d0a9e69e340a221a0f3ee3e93a9a7f01))
* Initial checkin that allows basic setup ([#1](https://github.com/momentohq/client-sdk-python/issues/1)) ([36ee5bf](https://github.com/momentohq/client-sdk-python/commit/36ee5bff911e6012ff9a0454ab7ec66107838c1d))
* list_concatenate_front() ([#227](https://github.com/momentohq/client-sdk-python/issues/227)) ([920470b](https://github.com/momentohq/client-sdk-python/commit/920470be383005157028b0ab9c4cfde9aad53616))
* make the retry logic configurable ([#299](https://github.com/momentohq/client-sdk-python/issues/299)) ([f2ec853](https://github.com/momentohq/client-sdk-python/commit/f2ec85333bc26e0abf6123e4a3458943d21c68b9))
* migrate synchronous API to consume gRPC synchronous API ([#163](https://github.com/momentohq/client-sdk-python/issues/163)) ([c9e1a34](https://github.com/momentohq/client-sdk-python/commit/c9e1a3495066b22d693e4612727ee01cab865e7d))
* Modernize the Python load-gen example. ([#168](https://github.com/momentohq/client-sdk-python/issues/168)) ([bc2f876](https://github.com/momentohq/client-sdk-python/commit/bc2f876cf55d89b4f5d9fa3d5004b783e481323a))
* mvi api wave 2 - upsert, similarity, and all metadata ([#383](https://github.com/momentohq/client-sdk-python/issues/383)) ([7aa55c7](https://github.com/momentohq/client-sdk-python/commit/7aa55c7fef1babef11b26979169baf8587afe1fc))
* mvi count items ([#427](https://github.com/momentohq/client-sdk-python/issues/427)) ([fcede36](https://github.com/momentohq/client-sdk-python/commit/fcede366534dd94e1869a9a4b856ef986500659e))
* mvi filter expressions ([#425](https://github.com/momentohq/client-sdk-python/issues/425)) ([a30db8f](https://github.com/momentohq/client-sdk-python/commit/a30db8f64ba7d96774ada7ce3eb32d0c357ee30d))
* mvi id in set filter expression ([#430](https://github.com/momentohq/client-sdk-python/issues/430)) ([1295167](https://github.com/momentohq/client-sdk-python/commit/1295167d9486631f8dc97b0891b33eb9a46559e5))
* new credential provider methods for accepting global api keys ([#512](https://github.com/momentohq/client-sdk-python/issues/512)) ([6809b6e](https://github.com/momentohq/client-sdk-python/commit/6809b6e6903ec74b0607ad45a6853cc5e93914da))
* Open source and publish package to PyPI ([#60](https://github.com/momentohq/client-sdk-python/issues/60)) ([824299a](https://github.com/momentohq/client-sdk-python/commit/824299a1333f9632aaea6cf64b67049548147711))
* order system example ([#458](https://github.com/momentohq/client-sdk-python/issues/458)) ([01fb509](https://github.com/momentohq/client-sdk-python/commit/01fb5092c16f167233805f5479fca6941fef88a8))
* promote project stability from alpha to beta ([#263](https://github.com/momentohq/client-sdk-python/issues/263)) ([b29992a](https://github.com/momentohq/client-sdk-python/commit/b29992a1a21f5ba9ce4629d9b1f8c3f8568f80d1))
* read version from package init ([#481](https://github.com/momentohq/client-sdk-python/issues/481)) ([6ca2525](https://github.com/momentohq/client-sdk-python/commit/6ca2525051892159db3673892fcac3cad08a567b))
* relax grpcio dependency lower bound ([#333](https://github.com/momentohq/client-sdk-python/issues/333)) ([ebcd70d](https://github.com/momentohq/client-sdk-python/commit/ebcd70d863134c3675cc74ea966e682077df3046))
* remove incubating, get-multi, and set-multi client-side apis ([#156](https://github.com/momentohq/client-sdk-python/issues/156)) ([0b53069](https://github.com/momentohq/client-sdk-python/commit/0b530698b1c820b7f238c49727d21c49999ea997))
* remove signing key APIs ([#195](https://github.com/momentohq/client-sdk-python/issues/195)) ([60df3d7](https://github.com/momentohq/client-sdk-python/commit/60df3d7be5e89248ce8206cb9bf0983c5b2e896e))
* remove vector client ([#450](https://github.com/momentohq/client-sdk-python/issues/450)) ([268068f](https://github.com/momentohq/client-sdk-python/commit/268068f0bdd315fafaccfb84fee4c71d8e67f023))
* rename cache client from `SimpleCacheClient` to `CacheClient` ([#304](https://github.com/momentohq/client-sdk-python/issues/304)) ([0c8ced9](https://github.com/momentohq/client-sdk-python/commit/0c8ced9496e81f63acfae528740783876b2d53e7))
* require protobuf 7 via momento-wire-types 0.135 ([#523](https://github.com/momentohq/client-sdk-python/issues/523)) ([49a7b36](https://github.com/momentohq/client-sdk-python/commit/49a7b367284318c53ee3ba50c7b6c3a20f861080))
* Set methods ([#235](https://github.com/momentohq/client-sdk-python/issues/235)) ([20e357f](https://github.com/momentohq/client-sdk-python/commit/20e357fb807108c500b35fc57c5afb243fa46821))
* set_if_not_exists() ([#243](https://github.com/momentohq/client-sdk-python/issues/243)) ([44b5f1c](https://github.com/momentohq/client-sdk-python/commit/44b5f1c9a251e29f5e0b3157dfb021b6d9b5fd36))
* Stop parsing ECacheResult.Ok ([#25](https://github.com/momentohq/client-sdk-python/issues/25)) ([b96ae03](https://github.com/momentohq/client-sdk-python/commit/b96ae03721991aaf90e2a9325768cdcb7d0519f1))
* support for multiple gRPC channels ([#125](https://github.com/momentohq/client-sdk-python/issues/125)) ([3606a77](https://github.com/momentohq/client-sdk-python/commit/3606a7767a5dcfc60a7c2e4595960aa44c390f99))
* support more vector item metadata value types ([#392](https://github.com/momentohq/client-sdk-python/issues/392)) ([7e1649c](https://github.com/momentohq/client-sdk-python/commit/7e1649cba9bcdf1cf620824358b0b0a33c249491))
* support mvi list indexes detail ([#408](https://github.com/momentohq/client-sdk-python/issues/408)) ([6b21cb2](https://github.com/momentohq/client-sdk-python/commit/6b21cb272f1f03764eb1b4cbe323c600afc8f82d))
* support protobuf&lt;3.20 ([#336](https://github.com/momentohq/client-sdk-python/issues/336)) ([a8c73c0](https://github.com/momentohq/client-sdk-python/commit/a8c73c021b855b5c1d974f1eac35b3cfc061a338))
* support topic sequence page ([#492](https://github.com/momentohq/client-sdk-python/issues/492)) ([d8e5039](https://github.com/momentohq/client-sdk-python/commit/d8e5039007f72794a23680cd53602b23076d71ad))
* use modern python type hint syntax ([#251](https://github.com/momentohq/client-sdk-python/issues/251)) ([c83ba8b](https://github.com/momentohq/client-sdk-python/commit/c83ba8b39795ebcaf6189b9abdd4b7bb42b686b2))
* Validate the CollectionTtl.ttl is positive. ([#224](https://github.com/momentohq/client-sdk-python/issues/224)) ([3a774d4](https://github.com/momentohq/client-sdk-python/commit/3a774d4a22147ba476acb6f31cb06f96c6dfb15f))
* wire method to get and set eager connection timeout ([#365](https://github.com/momentohq/client-sdk-python/issues/365)) ([5f82ba4](https://github.com/momentohq/client-sdk-python/commit/5f82ba4ffd70c6352f3288185b02cd2d0fc8d65b))


### Bug Fixes

* add back return statements for closed streams ([#356](https://github.com/momentohq/client-sdk-python/issues/356)) ([f2c9376](https://github.com/momentohq/client-sdk-python/commit/f2c93762f6dff6e28c541a93052d7153fbe96622))
* add env variables for integration tests ([#63](https://github.com/momentohq/client-sdk-python/issues/63)) ([433f694](https://github.com/momentohq/client-sdk-python/commit/433f69425ff8a6de2e56bf2689f4560a088d00dc))
* add GenerateDisposableToken Success and Error subclasses to responses init file ([#468](https://github.com/momentohq/client-sdk-python/issues/468)) ([6295cc6](https://github.com/momentohq/client-sdk-python/commit/6295cc66f7d6fde5ae072e3e7bb40a27935f9194))
* audit response hierarchy ([#298](https://github.com/momentohq/client-sdk-python/issues/298)) ([a67668a](https://github.com/momentohq/client-sdk-python/commit/a67668a23319b50e91fa058e85d866c12cc69cd7))
* bug where the __repr__ for Cred Provider would mutate it ([#316](https://github.com/momentohq/client-sdk-python/issues/316)) ([0cae587](https://github.com/momentohq/client-sdk-python/commit/0cae5871e0123a54f3dda8f00d9dbdf4e9ce7572))
* bump and pin libs to resolve grpc vuln ([#140](https://github.com/momentohq/client-sdk-python/issues/140)) ([2d1e829](https://github.com/momentohq/client-sdk-python/commit/2d1e82959e87e9570cf82197348d0a4314dd77eb))
* bump version ([#362](https://github.com/momentohq/client-sdk-python/issues/362)) ([9b6dbf5](https://github.com/momentohq/client-sdk-python/commit/9b6dbf52e91819b221fd2528086096a2fc6d55d1))
* change library python upper bound to 4 ([#331](https://github.com/momentohq/client-sdk-python/issues/331)) ([87947b4](https://github.com/momentohq/client-sdk-python/commit/87947b42507d1e846ab9ea15692d3d63efb43863))
* codegen import with one import from that is async ([#261](https://github.com/momentohq/client-sdk-python/issues/261)) ([92201ee](https://github.com/momentohq/client-sdk-python/commit/92201ee1d4152f3d18b334c17efd9b18176e0305))
* correct some issues with CI/CD jobs, move build to `tox` ([#49](https://github.com/momentohq/client-sdk-python/issues/49)) ([3b2afab](https://github.com/momentohq/client-sdk-python/commit/3b2afab41ab252d85191d6a8b948d9e0f76bdbda))
* disable dynamic DNS service config ([#497](https://github.com/momentohq/client-sdk-python/issues/497)) ([d677563](https://github.com/momentohq/client-sdk-python/commit/d677563afe032e83324c5aff8e64d20152f622a1))
* disable keepalive in grpc channel options for control plane clients and set grpc channel options in synchronous code ([#443](https://github.com/momentohq/client-sdk-python/issues/443)) ([3b14375](https://github.com/momentohq/client-sdk-python/commit/3b14375940af40548055007deb135fdc2671d5ae))
* do not use test util in package ([#464](https://github.com/momentohq/client-sdk-python/issues/464)) ([a558c0b](https://github.com/momentohq/client-sdk-python/commit/a558c0b814a70b4eb925897fc481df65561371f1))
* don't call retry logic on OK status codes ([#320](https://github.com/momentohq/client-sdk-python/issues/320)) ([7e93db2](https://github.com/momentohq/client-sdk-python/commit/7e93db22bc99fc0ffa41c5eed88e459cfc7f6428))
* Ensure test caches are cleaned up. ([#191](https://github.com/momentohq/client-sdk-python/issues/191)) ([284acea](https://github.com/momentohq/client-sdk-python/commit/284aceaf4fdb0de1d6f92e5d507f8029f484f761))
* fix sorted set fetch by score and add tests ([#359](https://github.com/momentohq/client-sdk-python/issues/359)) ([4b10ce3](https://github.com/momentohq/client-sdk-python/commit/4b10ce3cf60f48093ab5849d8ad4ffb6fe30d2cf))
* fix tests to use latest credential provider ([#238](https://github.com/momentohq/client-sdk-python/issues/238)) ([53bc313](https://github.com/momentohq/client-sdk-python/commit/53bc3136fe5d4b4d66e9ac094ad3db90ebfffe78))
* fix version of generate readme action ([#341](https://github.com/momentohq/client-sdk-python/issues/341)) ([d72c28e](https://github.com/momentohq/client-sdk-python/commit/d72c28e39cec9b127ce0bfa7771aa8f186fc09c2))
* Flatten exception hierarchy ([#44](https://github.com/momentohq/client-sdk-python/issues/44)) ([9a7fb98](https://github.com/momentohq/client-sdk-python/commit/9a7fb9864f4c6e84f9bbf92d5029849b5d505fca))
* Handle GRPC Error Codes. ([#42](https://github.com/momentohq/client-sdk-python/issues/42)) ([8f68a1f](https://github.com/momentohq/client-sdk-python/commit/8f68a1f62c5ec8615bc3d44228c533601ea5ad56))
* Increase sleep in tests to account for TTL precisions ([#53](https://github.com/momentohq/client-sdk-python/issues/53)) ([c4616fa](https://github.com/momentohq/client-sdk-python/commit/c4616fa767faf0da81241eab8d94451c617482b9))
* incubating dictionary convenience methods ([#104](https://github.com/momentohq/client-sdk-python/issues/104)) ([8bf614a](https://github.com/momentohq/client-sdk-python/commit/8bf614a9c2ad8599a1304140be44ffb2c50ebc6a))
* Integration tests ([#39](https://github.com/momentohq/client-sdk-python/issues/39)) ([41948be](https://github.com/momentohq/client-sdk-python/commit/41948be45730147d28a887eb2f998894437c824a))
* isolate test cache names on push to release ([#214](https://github.com/momentohq/client-sdk-python/issues/214)) ([9a16995](https://github.com/momentohq/client-sdk-python/commit/9a16995f97b555b680b1e820abe4a246cc50ad34))
* lint errors on 3.10+ for auth protos ([#479](https://github.com/momentohq/client-sdk-python/issues/479)) ([e3be891](https://github.com/momentohq/client-sdk-python/commit/e3be89171ff4284b68801a86320aedfd912b3a42))
* make sure to import GenerateDisposableToken response class before exporting it in init file ([#470](https://github.com/momentohq/client-sdk-python/issues/470)) ([e7ee744](https://github.com/momentohq/client-sdk-python/commit/e7ee7440e35bd4140c771d1ee4b5ac73f2e519f9))
* make ttl a required attribute ([#80](https://github.com/momentohq/client-sdk-python/issues/80)) ([ad76f68](https://github.com/momentohq/client-sdk-python/commit/ad76f68fa75573c7370b4106d7d6073a4edff7f8))
* Manual Workflow YAML :crossed_fingers ([#59](https://github.com/momentohq/client-sdk-python/issues/59)) ([483c7e7](https://github.com/momentohq/client-sdk-python/commit/483c7e71fad6804659c2f7d14c02824efbbfc0e1))
* Method Names and Add Code Hints ([#41](https://github.com/momentohq/client-sdk-python/issues/41)) ([462406b](https://github.com/momentohq/client-sdk-python/commit/462406bf763d85fb89c2d996ea3b1267bfab4149))
* mitigate m1 incompatibility ([#66](https://github.com/momentohq/client-sdk-python/issues/66)) ([9b3b6f7](https://github.com/momentohq/client-sdk-python/commit/9b3b6f7b5ffad563769d355577afadc0a9ff6c6f))
* momento logger function definitions ([#102](https://github.com/momentohq/client-sdk-python/issues/102)) ([7726aa6](https://github.com/momentohq/client-sdk-python/commit/7726aa61fbdae350dea7eb3ce74f1685d39765f8))
* pin ubuntu and poetry ci dependencies ([#165](https://github.com/momentohq/client-sdk-python/issues/165)) ([ff7512f](https://github.com/momentohq/client-sdk-python/commit/ff7512f4c2151dab0b6d6b41efbe10b338723075))
* prefer `importlib_metadata` to deprecated pkg_resources ([#340](https://github.com/momentohq/client-sdk-python/issues/340)) ([52a6ca3](https://github.com/momentohq/client-sdk-python/commit/52a6ca34cb6f859ab5b64353c0d62fb1a667abbd))
* public vs private library imports ([#297](https://github.com/momentohq/client-sdk-python/issues/297)) ([c1a24d5](https://github.com/momentohq/client-sdk-python/commit/c1a24d5ca4da0dd17a5ae52781c9779e3bcc210a))
* Push always uses default version ([#7](https://github.com/momentohq/client-sdk-python/issues/7)) ([a582ac9](https://github.com/momentohq/client-sdk-python/commit/a582ac96f7556a78987381ad5c9120310a3ab954))
* re-introduce retry interceptor for synchronous client ([#171](https://github.com/momentohq/client-sdk-python/issues/171)) ([a66746d](https://github.com/momentohq/client-sdk-python/commit/a66746da436567be94a46d30b91395ae1a77acee))
* remove client-side expiry check on disposable tokens ([#518](https://github.com/momentohq/client-sdk-python/issues/518)) ([60fbb19](https://github.com/momentohq/client-sdk-python/commit/60fbb19a06813114dbacfb75c3681cd57331f3a4))
* remove extraneous echo from make target ([#202](https://github.com/momentohq/client-sdk-python/issues/202)) ([e47c510](https://github.com/momentohq/client-sdk-python/commit/e47c5101a8eaabff8736bc2bb3f34f54d31e0720))
* remove manual release footgun ([#325](https://github.com/momentohq/client-sdk-python/issues/325)) ([be3aa30](https://github.com/momentohq/client-sdk-python/commit/be3aa30f9e10dc393c2579faaf4e553d83f1dac3))
* Return explicit message for OK Responses with incorrect state ([#12](https://github.com/momentohq/client-sdk-python/issues/12)) ([56817ab](https://github.com/momentohq/client-sdk-python/commit/56817ab5851604f8666d82860efed18815bb6485))
* Scalar cache response description of the request. ([#208](https://github.com/momentohq/client-sdk-python/issues/208)) ([1526f76](https://github.com/momentohq/client-sdk-python/commit/1526f767314afdbbbc32edf6195b25dd3c73503f))
* Services now return UNAUTHENTICATE instead of PERMISSION_DENIED ([#46](https://github.com/momentohq/client-sdk-python/issues/46)) ([529be21](https://github.com/momentohq/client-sdk-python/commit/529be213103b8099542cad36a27561ac8f1f24c4))
* set the release please base version in the manifest ([#476](https://github.com/momentohq/client-sdk-python/issues/476)) ([52f2f2b](https://github.com/momentohq/client-sdk-python/commit/52f2f2bfe2a2e275c2e8549bbbf1d6c852c952b9))
* spelling of "Framework" in project config ([#144](https://github.com/momentohq/client-sdk-python/issues/144)) ([11d849f](https://github.com/momentohq/client-sdk-python/commit/11d849f06d0fd048d6a6d52d35358bd3f9e8fc03))
* subscriptions bookkeeping in pubsub clients ([#510](https://github.com/momentohq/client-sdk-python/issues/510)) ([0d5e514](https://github.com/momentohq/client-sdk-python/commit/0d5e5144a3ad953990d83a1f880a35028c2d4ffc))
* timedelta to ms conversion and timedelta to deadline conversion ([#442](https://github.com/momentohq/client-sdk-python/issues/442)) ([14a84ca](https://github.com/momentohq/client-sdk-python/commit/14a84ca0bcae7f793e15a4c4d3c275ad19726715))
* typos in error logging ([#350](https://github.com/momentohq/client-sdk-python/issues/350)) ([d8bef74](https://github.com/momentohq/client-sdk-python/commit/d8bef74fd347c6bf882fa3fcdefee5425ff1afbd))
* Unpin dependencies to avoid library conflicts. ([#207](https://github.com/momentohq/client-sdk-python/issues/207)) ([f5c0c01](https://github.com/momentohq/client-sdk-python/commit/f5c0c01d4b5234161fbba69e0fe72c8e80b44783))
* update 'cryptography' pkg fix openssl vuln ([#150](https://github.com/momentohq/client-sdk-python/issues/150)) ([8f1aa9f](https://github.com/momentohq/client-sdk-python/commit/8f1aa9f6a23544b380454d2499cd1018510e321c))
* update example location ([#218](https://github.com/momentohq/client-sdk-python/issues/218)) ([6d53ce7](https://github.com/momentohq/client-sdk-python/commit/6d53ce73ffc9fe8f12ff331ad2f3e07edb5dbd9d))
* update poetry.lock, mostly to bump pyyaml to 6.0.1 ([#360](https://github.com/momentohq/client-sdk-python/issues/360)) ([e61be91](https://github.com/momentohq/client-sdk-python/commit/e61be91ec275f28cd308d65b35b3c0c76344c5f4))
* use != instead of is not ([#13](https://github.com/momentohq/client-sdk-python/issues/13)) ([5d961b8](https://github.com/momentohq/client-sdk-python/commit/5d961b82e8a8eb05ce77025aa8ea2e2e90969a60))
* Use relative imports ([#34](https://github.com/momentohq/client-sdk-python/issues/34)) ([c8103d4](https://github.com/momentohq/client-sdk-python/commit/c8103d4777f53c3664e74267efd8cdcd7990d61d))
* use relative imports everywhere in `src`, clean up test import ([#55](https://github.com/momentohq/client-sdk-python/issues/55)) ([17f533b](https://github.com/momentohq/client-sdk-python/commit/17f533bba6f140e248f82d7062178ce451d64134))
* validate vector index item metadata on serialization ([#391](https://github.com/momentohq/client-sdk-python/issues/391)) ([ec2b7c3](https://github.com/momentohq/client-sdk-python/commit/ec2b7c37f64391fd8f4bb57bc052f5cd4fc2faab))
* when detecting momento version also catch `ModuleNotFoundError` ([#459](https://github.com/momentohq/client-sdk-python/issues/459)) ([deebc69](https://github.com/momentohq/client-sdk-python/commit/deebc69ca35e7a3c975e003668f8b87191b86191))
* workaround to make our grpc interceptor more flexible ([#148](https://github.com/momentohq/client-sdk-python/issues/148)) ([f0c5ca3](https://github.com/momentohq/client-sdk-python/commit/f0c5ca3b0cd78ad33658467ffa0e4aced9382457))


### Miscellaneous

* add 'black' formatter to tox build environment ([#52](https://github.com/momentohq/client-sdk-python/issues/52)) ([4f2bb63](https://github.com/momentohq/client-sdk-python/commit/4f2bb63cf01bd8e432d004f37d56d59dab8b4260))
* Add `agent` metadata ([#75](https://github.com/momentohq/client-sdk-python/issues/75)) ([d2ebf0b](https://github.com/momentohq/client-sdk-python/commit/d2ebf0b4fb2387aefc57bc3c21eb2f39141dc15b))
* Add `CONTRIBUTING.md` and update `README.md` ([#64](https://github.com/momentohq/client-sdk-python/issues/64)) ([a8f8990](https://github.com/momentohq/client-sdk-python/commit/a8f89902c9f39194fa87c1e5e202308e4915d562))
* add a hardcoded example for prepy310 to the README template ([#259](https://github.com/momentohq/client-sdk-python/issues/259)) ([9341b2f](https://github.com/momentohq/client-sdk-python/commit/9341b2fae93e180a6bd6427b76f2cd549071ae94))
* add agent details and new runtime version header ([#456](https://github.com/momentohq/client-sdk-python/issues/456)) ([7a59011](https://github.com/momentohq/client-sdk-python/commit/7a59011ffbe8aa2dbebc398a936807ba13be0ecd))
* add caching patterns section ([#451](https://github.com/momentohq/client-sdk-python/issues/451)) ([9c3ef8c](https://github.com/momentohq/client-sdk-python/commit/9c3ef8c4dcfb5ddd2a7c406cec2a76129dde0ed1))
* add client instantiation example to docs ([#272](https://github.com/momentohq/client-sdk-python/issues/272)) ([a58f6c2](https://github.com/momentohq/client-sdk-python/commit/a58f6c27d40537edc17ce69e578d216c1ef8eeb2))
* add collection ttl data class ([#188](https://github.com/momentohq/client-sdk-python/issues/188)) ([461208e](https://github.com/momentohq/client-sdk-python/commit/461208eef9939394bfa364cf1620f4dc3762abda))
* add comments as fenceposts to reduce merge conflicts ([#201](https://github.com/momentohq/client-sdk-python/issues/201)) ([15a75ca](https://github.com/momentohq/client-sdk-python/commit/15a75ca2b49860f7b1dd9edafdd4c8ed1f7ff3f1))
* add doc examples for auth client ([#466](https://github.com/momentohq/client-sdk-python/issues/466)) ([0f6973e](https://github.com/momentohq/client-sdk-python/commit/0f6973e6d1b80a0408faa67dc14aad28b49b491f))
* add doc examples for the vector index client ([#390](https://github.com/momentohq/client-sdk-python/issues/390)) ([19d22c8](https://github.com/momentohq/client-sdk-python/commit/19d22c8741775b9a5e9f450f86c9d2dd605cd79a))
* add docs examples for using topics ([#382](https://github.com/momentohq/client-sdk-python/issues/382)) ([7f46449](https://github.com/momentohq/client-sdk-python/commit/7f4644946afc923dbfb420b63857be2151af2b04))
* Add incubating README and clean up incubating reprs. ([#105](https://github.com/momentohq/client-sdk-python/issues/105)) ([4290647](https://github.com/momentohq/client-sdk-python/commit/429064731ee48ddd2051f2a6c136357636556709))
* add lambda zip example ([#452](https://github.com/momentohq/client-sdk-python/issues/452)) ([c9c01ac](https://github.com/momentohq/client-sdk-python/commit/c9c01ac66246610722b70614f7831985cf2c2445)), closes [#819](https://github.com/momentohq/client-sdk-python/issues/819)
* add less verbose quickstart examples for README ([#306](https://github.com/momentohq/client-sdk-python/issues/306)) ([dd457a3](https://github.com/momentohq/client-sdk-python/commit/dd457a34d039cd51ab0669fa801ff4339ae5df06))
* add method documentation ([#15](https://github.com/momentohq/client-sdk-python/issues/15)) ([e053811](https://github.com/momentohq/client-sdk-python/commit/e0538115a94d06ca3166afa96a647b4af6b59df6))
* add module centralizing type hints ([#213](https://github.com/momentohq/client-sdk-python/issues/213)) ([8fc9444](https://github.com/momentohq/client-sdk-python/commit/8fc94449892511da6bc2aebe31a078c181c0b4de))
* add more mvi examples and move to examples folders ([#389](https://github.com/momentohq/client-sdk-python/issues/389)) ([c5a52d9](https://github.com/momentohq/client-sdk-python/commit/c5a52d93ce4f8e7ec0c6ed8b298933fdc0a64c45))
* add mvi filter expression example snippets ([#433](https://github.com/momentohq/client-sdk-python/issues/433)) ([80d26a8](https://github.com/momentohq/client-sdk-python/commit/80d26a8a1256361c399800584c3528653364d34e))
* add mypy type checker to build commands ([#51](https://github.com/momentohq/client-sdk-python/issues/51)) ([a17d2d0](https://github.com/momentohq/client-sdk-python/commit/a17d2d06cd73fc1536d96ae497f336195804861d))
* Add repr and str methods to response objects ([#113](https://github.com/momentohq/client-sdk-python/issues/113)) ([74dfdc6](https://github.com/momentohq/client-sdk-python/commit/74dfdc609c208f8d409e5fc5eedc274616507d40))
* Add rust requirement to CONTRIBUTING.md ([#271](https://github.com/momentohq/client-sdk-python/issues/271)) ([be43fc7](https://github.com/momentohq/client-sdk-python/commit/be43fc7d9c9de34bb038f198334eb900fb2584a0))
* add shared build step to pr build ([#136](https://github.com/momentohq/client-sdk-python/issues/136)) ([ac9ffd6](https://github.com/momentohq/client-sdk-python/commit/ac9ffd6210c824b1ce2e3b987240eb955cb71a50))
* add tests for the fixed count retry strategy ([#503](https://github.com/momentohq/client-sdk-python/issues/503)) ([ac7bdb8](https://github.com/momentohq/client-sdk-python/commit/ac7bdb8f59adc08000a9a509b4873777ad97fdc0))
* add timeout to topic subscribe ([#511](https://github.com/momentohq/client-sdk-python/issues/511)) ([0169714](https://github.com/momentohq/client-sdk-python/commit/01697140e479b67825c4d86bbfba9eab38969369))
* add trace level logging ([#302](https://github.com/momentohq/client-sdk-python/issues/302)) ([21cfaf2](https://github.com/momentohq/client-sdk-python/commit/21cfaf2d9a4f656c4b5ad29a7c04877a88b17f23))
* add v1 configurations ([#303](https://github.com/momentohq/client-sdk-python/issues/303)) ([1469fee](https://github.com/momentohq/client-sdk-python/commit/1469feece5807d73d95b7726917d2120d07fa638))
* Adding a rest keyword to the REST endpoint. ([#82](https://github.com/momentohq/client-sdk-python/issues/82)) ([faadcdb](https://github.com/momentohq/client-sdk-python/commit/faadcdb94d9f113c6acc1d717240fcd63a38d316))
* adding initial doc code samples ([#339](https://github.com/momentohq/client-sdk-python/issues/339)) ([16d0986](https://github.com/momentohq/client-sdk-python/commit/16d098615e6a1b0a2de8f75d29c4b8e98b91da47))
* adds code scanning workflow ([#152](https://github.com/momentohq/client-sdk-python/issues/152)) ([99618fd](https://github.com/momentohq/client-sdk-python/commit/99618fd54e0029f0802a5540f3e3f9525ac49f5a))
* adds dep scan on pr workflow ([#153](https://github.com/momentohq/client-sdk-python/issues/153)) ([3688fbe](https://github.com/momentohq/client-sdk-python/commit/3688fbe0f2f14ceb015c90b085cd22b73785d16e))
* align clients ([#220](https://github.com/momentohq/client-sdk-python/issues/220)) ([1b2f22c](https://github.com/momentohq/client-sdk-python/commit/1b2f22cc92434955456f7669af8eca69f80b31be))
* Align dictionary interface with latest spec ([#108](https://github.com/momentohq/client-sdk-python/issues/108)) ([0ac7534](https://github.com/momentohq/client-sdk-python/commit/0ac753438cc6c58455cd851843d690b6ed9669fa))
* bump examples to sdk 1.9.0 for MVI ([#375](https://github.com/momentohq/client-sdk-python/issues/375)) ([c60c47b](https://github.com/momentohq/client-sdk-python/commit/c60c47bd81baf4fbf6663dbff3afd7d5db064773))
* bump mypy dependency ([#416](https://github.com/momentohq/client-sdk-python/issues/416)) ([392c6e8](https://github.com/momentohq/client-sdk-python/commit/392c6e83447e849e222495bfed2884589f47d1a1))
* bump protos and consume mvi searchhit update ([#404](https://github.com/momentohq/client-sdk-python/issues/404)) ([a4a65ed](https://github.com/momentohq/client-sdk-python/commit/a4a65ed6fc4b970f9011b4364ea3aeec68b1689e))
* bump protos for count items uint64 ([#428](https://github.com/momentohq/client-sdk-python/issues/428)) ([5ba6988](https://github.com/momentohq/client-sdk-python/commit/5ba6988344aceec78ad490e694eade7ed011b390))
* bumps examples libs to latest ([#151](https://github.com/momentohq/client-sdk-python/issues/151)) ([2dc6a4c](https://github.com/momentohq/client-sdk-python/commit/2dc6a4c8755b503f3d63cc01c396e0fc9f88998e))
* clean up and raise new ConnectionException when eager connection fails ([#446](https://github.com/momentohq/client-sdk-python/issues/446)) ([34c5afd](https://github.com/momentohq/client-sdk-python/commit/34c5afd91557ad0560705b89990f4724ec4e87c6))
* **deps-dev:** bump @babel/traverse in /examples/lambda/infrastructure ([#413](https://github.com/momentohq/client-sdk-python/issues/413)) ([045d347](https://github.com/momentohq/client-sdk-python/commit/045d347c20ad97341b6e92a2f118b18082d85c8b))
* **deps-dev:** bump braces in /examples/lambda/infrastructure ([#484](https://github.com/momentohq/client-sdk-python/issues/484)) ([03601a1](https://github.com/momentohq/client-sdk-python/commit/03601a1e5faa877a98b1e9d28eb7fd9e6e9d1062))
* **deps:** bump momento from 0.16.3 to 0.17.0 in /examples ([#166](https://github.com/momentohq/client-sdk-python/issues/166)) ([b646bf5](https://github.com/momentohq/client-sdk-python/commit/b646bf5fe524149b41f001839c030cdc2646fffc))
* **deps:** bump momento from 0.17.0 to 0.18.1 in /examples ([#173](https://github.com/momentohq/client-sdk-python/issues/173)) ([a2e2a1d](https://github.com/momentohq/client-sdk-python/commit/a2e2a1d01ddec521c1b7702b06405de9d7d3b25c))
* **deps:** bump momento from 0.18.1 to 0.20.0 in /examples ([#217](https://github.com/momentohq/client-sdk-python/issues/217)) ([aeb2dc3](https://github.com/momentohq/client-sdk-python/commit/aeb2dc36b0b9940732d9352bb3e6d1af3d7a8e73))
* **deps:** bump momento from 0.20.0 to 0.21.0 in /examples ([047b2b2](https://github.com/momentohq/client-sdk-python/commit/047b2b2864bd66cd5a102c7db8121765ed92d78c))
* **deps:** bump momento from 0.20.0 to 0.21.0 in /examples ([#244](https://github.com/momentohq/client-sdk-python/issues/244)) ([047b2b2](https://github.com/momentohq/client-sdk-python/commit/047b2b2864bd66cd5a102c7db8121765ed92d78c))
* **deps:** bump momento from 0.21.0 to 0.22.0 in /examples ([#264](https://github.com/momentohq/client-sdk-python/issues/264)) ([b073c1a](https://github.com/momentohq/client-sdk-python/commit/b073c1aa233ed88024737321e20960a130eacd41))
* **deps:** bump momento from 1.1.1 to 1.3.1 in /examples ([#332](https://github.com/momentohq/client-sdk-python/issues/332)) ([71c0bd4](https://github.com/momentohq/client-sdk-python/commit/71c0bd4860b266ce0ff1678a2823672f3c212567))
* **deps:** bump momento from 1.13.2 to 1.15.1 in /examples ([#423](https://github.com/momentohq/client-sdk-python/issues/423)) ([1a82847](https://github.com/momentohq/client-sdk-python/commit/1a8284790dfca9ec1f399e30538e56d215a20d88))
* **deps:** bump momento from 1.17.0 to 1.18.0 in /examples ([#432](https://github.com/momentohq/client-sdk-python/issues/432)) ([eccbcfa](https://github.com/momentohq/client-sdk-python/commit/eccbcfa30cefed8d81fa74070f3700c73c8b16ba))
* **deps:** bump momento from 1.18.0 to 1.20.1 in /examples ([#447](https://github.com/momentohq/client-sdk-python/issues/447)) ([79bc60e](https://github.com/momentohq/client-sdk-python/commit/79bc60ef75ea098112403f91b11f2b221be58b14))
* **deps:** bump momento from 1.23.3 to 1.25.0 in /examples ([#493](https://github.com/momentohq/client-sdk-python/issues/493)) ([3e1d086](https://github.com/momentohq/client-sdk-python/commit/3e1d08636d087e309089b6d91810505ff9bb6b33))
* **deps:** bump momento from 1.25.0 to 1.26.0 in /examples ([#499](https://github.com/momentohq/client-sdk-python/issues/499)) ([a0a1382](https://github.com/momentohq/client-sdk-python/commit/a0a1382a66aa751354d19fd0f59d15388501729e))
* **deps:** bump momento from 1.26.0 to 1.27.0 in /examples ([#508](https://github.com/momentohq/client-sdk-python/issues/508)) ([62ac4ae](https://github.com/momentohq/client-sdk-python/commit/62ac4ae93bbd62a3583b91a76df570ea414d6096))
* **deps:** bump momento from 1.3.1 to 1.5.0 in /examples ([#337](https://github.com/momentohq/client-sdk-python/issues/337)) ([e94662a](https://github.com/momentohq/client-sdk-python/commit/e94662a8400b1fe9c5f55ab90d699ce08c8b71a4))
* **deps:** bump momento from 1.5.0 to 1.6.0 in /examples ([#348](https://github.com/momentohq/client-sdk-python/issues/348)) ([4700466](https://github.com/momentohq/client-sdk-python/commit/47004669c53a5ad2909388b4e2dcb5323c89ab85))
* eager connection should fail fast ([#445](https://github.com/momentohq/client-sdk-python/issues/445)) ([3c97c40](https://github.com/momentohq/client-sdk-python/commit/3c97c4061c25406c1252a4f1958cea373a5471e4))
* empty commit to test build ([cc82895](https://github.com/momentohq/client-sdk-python/commit/cc828958cdd06d5f3daca80efa33094a380f9c63))
* enable linting on tests ([#266](https://github.com/momentohq/client-sdk-python/issues/266)) ([7aa143b](https://github.com/momentohq/client-sdk-python/commit/7aa143bcfcdec4e7a149775565db45a8b4462a7a))
* explicitly test behavior when using default similarity metric ([#393](https://github.com/momentohq/client-sdk-python/issues/393)) ([fd901d0](https://github.com/momentohq/client-sdk-python/commit/fd901d01d747b6f621c17c5606a572fcae070fc2))
* fall back manual version when dist-info absent ([#462](https://github.com/momentohq/client-sdk-python/issues/462)) ([2ccc50b](https://github.com/momentohq/client-sdk-python/commit/2ccc50b9abf87377d1fee6c90b7ac1e9db45c4dc))
* fix as-bytes error messages ([#273](https://github.com/momentohq/client-sdk-python/issues/273)) ([0e79b0e](https://github.com/momentohq/client-sdk-python/commit/0e79b0e1c0f17ca754f5c430bb8993f2bcbe1d5d))
* fix markdown in lambda zip example ([#453](https://github.com/momentohq/client-sdk-python/issues/453)) ([8ceff0f](https://github.com/momentohq/client-sdk-python/commit/8ceff0fbb7463e5a2e78c78192a6c7f07af76222))
* fix tests for python 3.7, add 3.7 to CI test matrix ([#54](https://github.com/momentohq/client-sdk-python/issues/54)) ([0f67b6b](https://github.com/momentohq/client-sdk-python/commit/0f67b6b97b70761c1b767c9fcfafa6c8c0f64452))
* fixed timeout retry strategy ([#504](https://github.com/momentohq/client-sdk-python/issues/504)) ([5f1398f](https://github.com/momentohq/client-sdk-python/commit/5f1398fc45551c4afbb4180395e0166872a63f8c))
* improve imports for configurations and CredentialProvider ([#295](https://github.com/momentohq/client-sdk-python/issues/295)) ([c59217b](https://github.com/momentohq/client-sdk-python/commit/c59217b35a6fc917ee51b5b0d4c6c6a710be820b))
* improve resource exhausted error message ([#485](https://github.com/momentohq/client-sdk-python/issues/485)) ([b4439bd](https://github.com/momentohq/client-sdk-python/commit/b4439bd1b707a450b9c02a1b821579111105b115))
* include search_and_fetch_vectors in mvi api snippets ([#406](https://github.com/momentohq/client-sdk-python/issues/406)) ([51c54db](https://github.com/momentohq/client-sdk-python/commit/51c54dba7ab51e47c125c67363e74b3408ba9a75))
* **main:** release 1.23.5 ([#474](https://github.com/momentohq/client-sdk-python/issues/474)) ([4b4aeb8](https://github.com/momentohq/client-sdk-python/commit/4b4aeb8a443a697f229af9ad396715ece32b5f47))
* **main:** release 1.24.0 ([#480](https://github.com/momentohq/client-sdk-python/issues/480)) ([a3f7b6c](https://github.com/momentohq/client-sdk-python/commit/a3f7b6c6cc129f7604920f8d212adce7ab7661d5))
* **main:** release 1.25.0 ([#491](https://github.com/momentohq/client-sdk-python/issues/491)) ([ec6eaaa](https://github.com/momentohq/client-sdk-python/commit/ec6eaaa305d27692466da297e377d508a97a4bbd))
* **main:** release 1.26.0 ([#494](https://github.com/momentohq/client-sdk-python/issues/494)) ([50df735](https://github.com/momentohq/client-sdk-python/commit/50df73575326d1c5f221132b2f4d052e7c0e561a))
* **main:** release 1.27.0 ([#500](https://github.com/momentohq/client-sdk-python/issues/500)) ([1443545](https://github.com/momentohq/client-sdk-python/commit/1443545476530d877ebb16821910b17e95080fc2))
* **main:** release 1.28.0 ([#509](https://github.com/momentohq/client-sdk-python/issues/509)) ([d34001b](https://github.com/momentohq/client-sdk-python/commit/d34001bd65ee0e5446bf1ea3ddfc5e6b2d7b8e77))
* **main:** release 1.28.1 ([#516](https://github.com/momentohq/client-sdk-python/issues/516)) ([26b4bd6](https://github.com/momentohq/client-sdk-python/commit/26b4bd6081dec0d963471f392ddfdc3c26b2238f))
* **main:** release 2.0.0 ([#521](https://github.com/momentohq/client-sdk-python/issues/521)) ([a13f5f8](https://github.com/momentohq/client-sdk-python/commit/a13f5f86692c728e9ffbee95a976bafe49e4a536))
* migrate mypy config to pyproject.toml and simplify ([#234](https://github.com/momentohq/client-sdk-python/issues/234)) ([feff925](https://github.com/momentohq/client-sdk-python/commit/feff92525222307fab5377dc33e36ae32b355e82))
* more examples and readme updates ([#517](https://github.com/momentohq/client-sdk-python/issues/517)) ([cc9378f](https://github.com/momentohq/client-sdk-python/commit/cc9378f439c4064ea6dcbba7801cf6736d33d99f))
* move Python SDK examples from `client-sdk-examples` repo ([#139](https://github.com/momentohq/client-sdk-python/issues/139)) ([47f43ca](https://github.com/momentohq/client-sdk-python/commit/47f43cabdc99bd71a18ec2f9a71b5f0f19736fb9))
* organize project ([#183](https://github.com/momentohq/client-sdk-python/issues/183)) ([1c9a935](https://github.com/momentohq/client-sdk-python/commit/1c9a935fb74bc2245ed9bc20ae742667d2d804f9))
* pin cryptography dependency ([#170](https://github.com/momentohq/client-sdk-python/issues/170)) ([e94ad18](https://github.com/momentohq/client-sdk-python/commit/e94ad183494054b9ffe061c7f836d4233ec5b976))
* prefer API key over AUTH token terminology ([#385](https://github.com/momentohq/client-sdk-python/issues/385)) ([4214a70](https://github.com/momentohq/client-sdk-python/commit/4214a7014c7729c7f1f9b7d8840e5eab46fb8129))
* refactor credential provider, add string provider ([#226](https://github.com/momentohq/client-sdk-python/issues/226)) ([70f7a94](https://github.com/momentohq/client-sdk-python/commit/70f7a94036e79746134f671d524c8ccd3299cee7))
* Refactor multi-get and multi-set ([#112](https://github.com/momentohq/client-sdk-python/issues/112)) ([ecb8e0e](https://github.com/momentohq/client-sdk-python/commit/ecb8e0e46add5aac26adcbaa8656948e4698c777))
* refactor sync client to be based on async client ([#56](https://github.com/momentohq/client-sdk-python/issues/56)) ([ed2fdb6](https://github.com/momentohq/client-sdk-python/commit/ed2fdb6c3c5735981e2ea564b64f5b84f92b296c))
* refactor tests into smaller files ([#181](https://github.com/momentohq/client-sdk-python/issues/181)) ([e5f4680](https://github.com/momentohq/client-sdk-python/commit/e5f4680f070d40e6bda9ba66cef2ae7f2b0c77ea))
* refine str and repr for collections ([#265](https://github.com/momentohq/client-sdk-python/issues/265)) ([16cd9c8](https://github.com/momentohq/client-sdk-python/commit/16cd9c8b5fe87787cb63a758887c8aad4bec2bb1))
* regen examples requirements.txt ([#146](https://github.com/momentohq/client-sdk-python/issues/146)) ([c57624c](https://github.com/momentohq/client-sdk-python/commit/c57624c2544717bd3534371455076c124795bbaf))
* regen requirements.txt for examples ([#147](https://github.com/momentohq/client-sdk-python/issues/147)) ([50a6956](https://github.com/momentohq/client-sdk-python/commit/50a69563683fd8e37cfe9707121029192f3a5f21))
* release 1.23.4 ([#475](https://github.com/momentohq/client-sdk-python/issues/475)) ([6db8d8c](https://github.com/momentohq/client-sdk-python/commit/6db8d8c25bc5a0e7411c3db9237a75b032d490c5))
* release 1.29.0 ([#527](https://github.com/momentohq/client-sdk-python/issues/527)) ([20241e5](https://github.com/momentohq/client-sdk-python/commit/20241e5f881485a0bb015a57d7390de7a1d78c27))
* release-please workflow should pick up feat, fix, and chore commits ([#486](https://github.com/momentohq/client-sdk-python/issues/486)) ([6e975cb](https://github.com/momentohq/client-sdk-python/commit/6e975cb4dea071147573cb18dce2d2af4b3f8878))
* Remove compatibility verbiage from README ([#192](https://github.com/momentohq/client-sdk-python/issues/192)) ([d3dc2d0](https://github.com/momentohq/client-sdk-python/commit/d3dc2d0c61ef3a795f1f72fb2f4e5653cf69160f))
* remove configuration bases and make middleware more pythonic ([#505](https://github.com/momentohq/client-sdk-python/issues/505)) ([6b810cc](https://github.com/momentohq/client-sdk-python/commit/6b810cca96a3d170adb13de494b3bed267f995f9))
* remove obsolete release workflows ([#482](https://github.com/momentohq/client-sdk-python/issues/482)) ([5b76c65](https://github.com/momentohq/client-sdk-python/commit/5b76c6522a127449c0c6ea91fb7549d9a7f8b25c))
* remove pagination from list caches and list signing keys ([#270](https://github.com/momentohq/client-sdk-python/issues/270)) ([118659e](https://github.com/momentohq/client-sdk-python/commit/118659edaf472c06c8b2afb9b4770ec485c1ae08))
* remove unnecessary score alias ([#409](https://github.com/momentohq/client-sdk-python/issues/409)) ([ea9bf4d](https://github.com/momentohq/client-sdk-python/commit/ea9bf4d9e094c7a07d19764ac5b6d75fef6f6117))
* rename "multi" methods and responses. ([#118](https://github.com/momentohq/client-sdk-python/issues/118)) ([1afec19](https://github.com/momentohq/client-sdk-python/commit/1afec1905509813523995b58cb117006bf237d28))
* restrict importlib_metadata package to python &lt; 3.8 ([#343](https://github.com/momentohq/client-sdk-python/issues/343)) ([5276f1f](https://github.com/momentohq/client-sdk-python/commit/5276f1f743c950fc2a1ccfa41b2a03e017b50957))
* retire the machine-user release token ([#520](https://github.com/momentohq/client-sdk-python/issues/520)) ([3b6a220](https://github.com/momentohq/client-sdk-python/commit/3b6a2205eb61958093627f22d07712d7df47b9a5))
* send only-once headers per client instead of just once ([#507](https://github.com/momentohq/client-sdk-python/issues/507)) ([11da1a5](https://github.com/momentohq/client-sdk-python/commit/11da1a5c20f483b6772c675a51a02f6d7b786df9))
* set release-please base version as 1.23.4 ([#477](https://github.com/momentohq/client-sdk-python/issues/477)) ([06869b2](https://github.com/momentohq/client-sdk-python/commit/06869b2316406b875bcdf2535b13c373ae8cecfe))
* specify path to release-please manifest ([#487](https://github.com/momentohq/client-sdk-python/issues/487)) ([227aa40](https://github.com/momentohq/client-sdk-python/commit/227aa40697d3604ef7d720e52aabdaec348855dc))
* switch to using README template generator ([#196](https://github.com/momentohq/client-sdk-python/issues/196)) ([f446129](https://github.com/momentohq/client-sdk-python/commit/f446129df9ec079923aaf88c3018628aaf10afcd))
* temporarily add score alias for backward compatibility ([#405](https://github.com/momentohq/client-sdk-python/issues/405)) ([b90dc8d](https://github.com/momentohq/client-sdk-python/commit/b90dc8d4219aeaf15007326170781ae978e00f6d))
* undo public type aliases ([#252](https://github.com/momentohq/client-sdk-python/issues/252)) ([40f2277](https://github.com/momentohq/client-sdk-python/commit/40f22779dcd66f0b9bd7644c141fc1581289f982))
* update all examples to use factory method ([#372](https://github.com/momentohq/client-sdk-python/issues/372)) ([74cac84](https://github.com/momentohq/client-sdk-python/commit/74cac84f5c9b116605c09488987688d57e6bab90))
* update codegen to handle examples in docstrings ([#225](https://github.com/momentohq/client-sdk-python/issues/225)) ([72f6fad](https://github.com/momentohq/client-sdk-python/commit/72f6fad02e6f0d61078339661380cbb53346d1d7))
* update CONTRIBUTING.md on how to add a new dependency ([#77](https://github.com/momentohq/client-sdk-python/issues/77)) ([f83bf46](https://github.com/momentohq/client-sdk-python/commit/f83bf462fca02d6a1f50c726c4a9d51f2f9f3c2e))
* update doc examples for `get item` methods ([#424](https://github.com/momentohq/client-sdk-python/issues/424)) ([a548f75](https://github.com/momentohq/client-sdk-python/commit/a548f758683a8e8b20f661e6eb4c0973cb0e8962))
* update example snippets for mvi count items ([#429](https://github.com/momentohq/client-sdk-python/issues/429)) ([a754c1b](https://github.com/momentohq/client-sdk-python/commit/a754c1b23d43aa80501a0947c76596d05602efc0))
* update examples ([#305](https://github.com/momentohq/client-sdk-python/issues/305)) ([bda4096](https://github.com/momentohq/client-sdk-python/commit/bda4096363182632ed60f1b6a74402046c2712aa))
* update examples for 1.7.0 ([#354](https://github.com/momentohq/client-sdk-python/issues/354)) ([df6c9d0](https://github.com/momentohq/client-sdk-python/commit/df6c9d094f52087d8ed8c9dd17f29254679e9597))
* update examples for mvi updates ([#403](https://github.com/momentohq/client-sdk-python/issues/403)) ([0df1927](https://github.com/momentohq/client-sdk-python/commit/0df1927a9fbb3352e6906eaf9a18adaad86ae7d6))
* update examples to use latest python client ([#197](https://github.com/momentohq/client-sdk-python/issues/197)) ([a200f21](https://github.com/momentohq/client-sdk-python/commit/a200f21ba3eca60bedb54dfe4e1cd5a92bbd0d69))
* update examples to use SDK version 1.1.1 ([#321](https://github.com/momentohq/client-sdk-python/issues/321)) ([10c8113](https://github.com/momentohq/client-sdk-python/commit/10c8113cfaca04b91fd1c8ff6474697b104f943f))
* update license file ([#488](https://github.com/momentohq/client-sdk-python/issues/488)) ([bb61d81](https://github.com/momentohq/client-sdk-python/commit/bb61d81653921952022337fb608d645f6a67924d))
* update list of retryable gRPC functions ([#495](https://github.com/momentohq/client-sdk-python/issues/495)) ([b8e527c](https://github.com/momentohq/client-sdk-python/commit/b8e527c8c3b6335ecf1966cc7cd63702088e3f99))
* update loadgen to be more specific for numberOfConcurrentRequests caveat ([#396](https://github.com/momentohq/client-sdk-python/issues/396)) ([9aba4df](https://github.com/momentohq/client-sdk-python/commit/9aba4df5941a7dfdcdeecc0995d2113373c312b6))
* update momento version in examples ([#471](https://github.com/momentohq/client-sdk-python/issues/471)) ([fd0fd34](https://github.com/momentohq/client-sdk-python/commit/fd0fd343e88fc12430358ab3cb3fe5e23c49ab76))
* update momento-wire-types, adjust pb/grpc type hints, and refactor requests ([#335](https://github.com/momentohq/client-sdk-python/issues/335)) ([49c1754](https://github.com/momentohq/client-sdk-python/commit/49c1754a9feb56d2117eb2fad514188f9d0d1db2))
* update mvi backend for create/list indexes updates ([#407](https://github.com/momentohq/client-sdk-python/issues/407)) ([f4be059](https://github.com/momentohq/client-sdk-python/commit/f4be0598d1cfa2ac77e8d28fd3f118b8329338b4))
* update protos to use latest get item and get item metadata rpcs ([#420](https://github.com/momentohq/client-sdk-python/issues/420)) ([850b201](https://github.com/momentohq/client-sdk-python/commit/850b201ec8cb44d8ee5d45c1066a2109f989ba5d))
* update python SDK to release from release branch ([#126](https://github.com/momentohq/client-sdk-python/issues/126)) ([27ef36f](https://github.com/momentohq/client-sdk-python/commit/27ef36f7b3f090a6ea288bec8e6231dcf40834f6))
* update README.md to include M1 workaround ([#76](https://github.com/momentohq/client-sdk-python/issues/76)) ([07f056d](https://github.com/momentohq/client-sdk-python/commit/07f056dfc13c92e66e532396a64eeada73ba95d9))
* update secret variable name for PyPI auth token ([#68](https://github.com/momentohq/client-sdk-python/issues/68)) ([8ac3a74](https://github.com/momentohq/client-sdk-python/commit/8ac3a74298d34cb527f9fcba0ad4a852a293d9a6))
* Update the Momento wire types dependency ([#323](https://github.com/momentohq/client-sdk-python/issues/323)) ([91b85ce](https://github.com/momentohq/client-sdk-python/commit/91b85cead369147757f04aeb686f37700ef7b5a0))
* update topic subscription response ([#381](https://github.com/momentohq/client-sdk-python/issues/381)) ([7e7c477](https://github.com/momentohq/client-sdk-python/commit/7e7c4775e703402faf79fa625f5c47347d754449))
* update vector index example ([#388](https://github.com/momentohq/client-sdk-python/issues/388)) ([dbc78a9](https://github.com/momentohq/client-sdk-python/commit/dbc78a9eca18d9b3fdab89c9855ec30ff59bb0f9))
* update vector index examples ([#384](https://github.com/momentohq/client-sdk-python/issues/384)) ([08865c2](https://github.com/momentohq/client-sdk-python/commit/08865c286a811403f26bec7e926c8ee8d498406f))
* update workflows, examples, tests, readmes for api keys v2 ([#514](https://github.com/momentohq/client-sdk-python/issues/514)) ([2c8738f](https://github.com/momentohq/client-sdk-python/commit/2c8738fed0ec142a9a8b1ffe0c712547c7b04585))
* updates examples to latest syntax ([#145](https://github.com/momentohq/client-sdk-python/issues/145)) ([b0bc491](https://github.com/momentohq/client-sdk-python/commit/b0bc4911c1fb36584794e4178cb56d0177b4a147))
* updating README to use factory method ([#374](https://github.com/momentohq/client-sdk-python/issues/374)) ([1624253](https://github.com/momentohq/client-sdk-python/commit/162425347230339ac0b3179c4e9f5cc8dc7b72bf))
* upgrade project classifier to stable ([#311](https://github.com/momentohq/client-sdk-python/issues/311)) ([10e852e](https://github.com/momentohq/client-sdk-python/commit/10e852e256481d0ca55c574ee854943e5c2f6adb))
* upgrade project stability to stable ([#309](https://github.com/momentohq/client-sdk-python/issues/309)) ([4ba7acf](https://github.com/momentohq/client-sdk-python/commit/4ba7acfc4b247fb3172ecfc302666fa25b7afd3b))
* upgrade proto dependency version ([#489](https://github.com/momentohq/client-sdk-python/issues/489)) ([77b855f](https://github.com/momentohq/client-sdk-python/commit/77b855f9dec8311c5830e6189200a9a9de7b08d7))
* use guids in tests ([#132](https://github.com/momentohq/client-sdk-python/issues/132)) ([4ba8c73](https://github.com/momentohq/client-sdk-python/commit/4ba8c73bdcf1d04d1b37471af0222c9d9dd866dd))
* use poetry ([#133](https://github.com/momentohq/client-sdk-python/issues/133)) ([32d15cf](https://github.com/momentohq/client-sdk-python/commit/32d15cfcd0819a0b6eafa2f4b7a39a591870653c))
* use property for next data client ([#216](https://github.com/momentohq/client-sdk-python/issues/216)) ([e0a7bfb](https://github.com/momentohq/client-sdk-python/commit/e0a7bfbaa8029b9b6a2751031ed23d2800e15b1d))
* use ruff formatter and remove black ([#411](https://github.com/momentohq/client-sdk-python/issues/411)) ([7f0da81](https://github.com/momentohq/client-sdk-python/commit/7f0da81ca18eafc648067a3e0c9f1f23be85890c))
* use ruff in examples ([#412](https://github.com/momentohq/client-sdk-python/issues/412)) ([6c30e7d](https://github.com/momentohq/client-sdk-python/commit/6c30e7d00154fabb6a62d0bfdf37876d28e25b1e))
* use ruff linter ([#410](https://github.com/momentohq/client-sdk-python/issues/410)) ([df0b943](https://github.com/momentohq/client-sdk-python/commit/df0b943cae723c717524811b5c0f112797ca93d4))
* use ubuntu-24.04 for ci where possible ([#454](https://github.com/momentohq/client-sdk-python/issues/454)) ([512d982](https://github.com/momentohq/client-sdk-python/commit/512d982256bffa9ae2680d210fb07c726b36ea6c))
* use vector endpoint for MVI API calls ([#373](https://github.com/momentohq/client-sdk-python/issues/373)) ([6f6e0f8](https://github.com/momentohq/client-sdk-python/commit/6f6e0f81dfb366e2178877331dba64203dba4efc))

## [2.0.0](https://github.com/momentohq/client-sdk-python/compare/v1.28.1...v2.0.0) (2026-09-01)


### ⚠ BREAKING CHANGES

* requires momento-wire-types >=0.135.0 and protobuf >=7.35.1,<8, and raises the minimum Python to 3.10. Support for protobuf 3 and 4 is removed.

### Features

* require protobuf 7 via momento-wire-types 0.135 ([#523](https://github.com/momentohq/client-sdk-python/issues/523)) ([49a7b36](https://github.com/momentohq/client-sdk-python/commit/49a7b367284318c53ee3ba50c7b6c3a20f861080))


### Miscellaneous

* retire the machine-user release token ([#520](https://github.com/momentohq/client-sdk-python/issues/520)) ([3b6a220](https://github.com/momentohq/client-sdk-python/commit/3b6a2205eb61958093627f22d07712d7df47b9a5))

## [1.28.1](https://github.com/momentohq/client-sdk-python/compare/v1.28.0...v1.28.1) (2026-02-04)


### Bug Fixes

* remove client-side expiry check on disposable tokens ([#518](https://github.com/momentohq/client-sdk-python/issues/518)) ([60fbb19](https://github.com/momentohq/client-sdk-python/commit/60fbb19a06813114dbacfb75c3681cd57331f3a4))


### Miscellaneous

* more examples and readme updates ([#517](https://github.com/momentohq/client-sdk-python/issues/517)) ([cc9378f](https://github.com/momentohq/client-sdk-python/commit/cc9378f439c4064ea6dcbba7801cf6736d33d99f))
* update workflows, examples, tests, readmes for api keys v2 ([#514](https://github.com/momentohq/client-sdk-python/issues/514)) ([2c8738f](https://github.com/momentohq/client-sdk-python/commit/2c8738fed0ec142a9a8b1ffe0c712547c7b04585))

## [1.28.0](https://github.com/momentohq/client-sdk-python/compare/v1.27.0...v1.28.0) (2025-12-17)


### Features

* new credential provider methods for accepting global api keys ([#512](https://github.com/momentohq/client-sdk-python/issues/512)) ([6809b6e](https://github.com/momentohq/client-sdk-python/commit/6809b6e6903ec74b0607ad45a6853cc5e93914da))


### Bug Fixes

* subscriptions bookkeeping in pubsub clients ([#510](https://github.com/momentohq/client-sdk-python/issues/510)) ([0d5e514](https://github.com/momentohq/client-sdk-python/commit/0d5e5144a3ad953990d83a1f880a35028c2d4ffc))


### Miscellaneous

* add timeout to topic subscribe ([#511](https://github.com/momentohq/client-sdk-python/issues/511)) ([0169714](https://github.com/momentohq/client-sdk-python/commit/01697140e479b67825c4d86bbfba9eab38969369))
* **deps:** bump momento from 1.26.0 to 1.27.0 in /examples ([#508](https://github.com/momentohq/client-sdk-python/issues/508)) ([62ac4ae](https://github.com/momentohq/client-sdk-python/commit/62ac4ae93bbd62a3583b91a76df570ea414d6096))

## [1.27.0](https://github.com/momentohq/client-sdk-python/compare/v1.26.0...v1.27.0) (2025-05-07)


### Features

* Add a credential provider for Momento Local ([#501](https://github.com/momentohq/client-sdk-python/issues/501)) ([9f809c8](https://github.com/momentohq/client-sdk-python/commit/9f809c84bfbf35ce8576da773f419b7df741da20))
* add middleware ([#502](https://github.com/momentohq/client-sdk-python/issues/502)) ([1230510](https://github.com/momentohq/client-sdk-python/commit/1230510154681507a8cb6a58a28ffde8038e12e8))


### Miscellaneous

* add tests for the fixed count retry strategy ([#503](https://github.com/momentohq/client-sdk-python/issues/503)) ([ac7bdb8](https://github.com/momentohq/client-sdk-python/commit/ac7bdb8f59adc08000a9a509b4873777ad97fdc0))
* **deps:** bump momento from 1.25.0 to 1.26.0 in /examples ([#499](https://github.com/momentohq/client-sdk-python/issues/499)) ([a0a1382](https://github.com/momentohq/client-sdk-python/commit/a0a1382a66aa751354d19fd0f59d15388501729e))
* fixed timeout retry strategy ([#504](https://github.com/momentohq/client-sdk-python/issues/504)) ([5f1398f](https://github.com/momentohq/client-sdk-python/commit/5f1398fc45551c4afbb4180395e0166872a63f8c))
* remove configuration bases and make middleware more pythonic ([#505](https://github.com/momentohq/client-sdk-python/issues/505)) ([6b810cc](https://github.com/momentohq/client-sdk-python/commit/6b810cca96a3d170adb13de494b3bed267f995f9))
* send only-once headers per client instead of just once ([#507](https://github.com/momentohq/client-sdk-python/issues/507)) ([11da1a5](https://github.com/momentohq/client-sdk-python/commit/11da1a5c20f483b6772c675a51a02f6d7b786df9))

## [1.26.0](https://github.com/momentohq/client-sdk-python/compare/v1.25.0...v1.26.0) (2025-03-06)


### Features

* add topic grpc config and transport strategy and make sure publish deadline is set ([#496](https://github.com/momentohq/client-sdk-python/issues/496)) ([bfac90d](https://github.com/momentohq/client-sdk-python/commit/bfac90d0c235ef79272ddbb54ad80b3ae7d65c82))


### Bug Fixes

* disable dynamic DNS service config ([#497](https://github.com/momentohq/client-sdk-python/issues/497)) ([d677563](https://github.com/momentohq/client-sdk-python/commit/d677563afe032e83324c5aff8e64d20152f622a1))


### Miscellaneous

* **deps:** bump momento from 1.23.3 to 1.25.0 in /examples ([#493](https://github.com/momentohq/client-sdk-python/issues/493)) ([3e1d086](https://github.com/momentohq/client-sdk-python/commit/3e1d08636d087e309089b6d91810505ff9bb6b33))
* update list of retryable gRPC functions ([#495](https://github.com/momentohq/client-sdk-python/issues/495)) ([b8e527c](https://github.com/momentohq/client-sdk-python/commit/b8e527c8c3b6335ecf1966cc7cd63702088e3f99))

## [1.25.0](https://github.com/momentohq/client-sdk-python/compare/v1.24.0...v1.25.0) (2024-11-21)


### Features

* support topic sequence page ([#492](https://github.com/momentohq/client-sdk-python/issues/492)) ([d8e5039](https://github.com/momentohq/client-sdk-python/commit/d8e5039007f72794a23680cd53602b23076d71ad))


### Miscellaneous

* **deps-dev:** bump braces in /examples/lambda/infrastructure ([#484](https://github.com/momentohq/client-sdk-python/issues/484)) ([03601a1](https://github.com/momentohq/client-sdk-python/commit/03601a1e5faa877a98b1e9d28eb7fd9e6e9d1062))
* improve resource exhausted error message ([#485](https://github.com/momentohq/client-sdk-python/issues/485)) ([b4439bd](https://github.com/momentohq/client-sdk-python/commit/b4439bd1b707a450b9c02a1b821579111105b115))
* release-please workflow should pick up feat, fix, and chore commits ([#486](https://github.com/momentohq/client-sdk-python/issues/486)) ([6e975cb](https://github.com/momentohq/client-sdk-python/commit/6e975cb4dea071147573cb18dce2d2af4b3f8878))
* specify path to release-please manifest ([#487](https://github.com/momentohq/client-sdk-python/issues/487)) ([227aa40](https://github.com/momentohq/client-sdk-python/commit/227aa40697d3604ef7d720e52aabdaec348855dc))
* update license file ([#488](https://github.com/momentohq/client-sdk-python/issues/488)) ([bb61d81](https://github.com/momentohq/client-sdk-python/commit/bb61d81653921952022337fb608d645f6a67924d))
* upgrade proto dependency version ([#489](https://github.com/momentohq/client-sdk-python/issues/489)) ([77b855f](https://github.com/momentohq/client-sdk-python/commit/77b855f9dec8311c5830e6189200a9a9de7b08d7))

## [1.24.0](https://github.com/momentohq/client-sdk-python/compare/v1.23.5...v1.24.0) (2024-09-27)


### Features

* read version from package init ([#481](https://github.com/momentohq/client-sdk-python/issues/481)) ([6ca2525](https://github.com/momentohq/client-sdk-python/commit/6ca2525051892159db3673892fcac3cad08a567b))


### Bug Fixes

* lint errors on 3.10+ for auth protos ([#479](https://github.com/momentohq/client-sdk-python/issues/479)) ([e3be891](https://github.com/momentohq/client-sdk-python/commit/e3be89171ff4284b68801a86320aedfd912b3a42))

## [1.23.5](https://github.com/momentohq/client-sdk-python/compare/v1.23.4...v1.23.5) (2024-09-27)


### Miscellaneous Chores

* set release-please base version as 1.23.4 ([#477](https://github.com/momentohq/client-sdk-python/issues/477)) ([06869b2](https://github.com/momentohq/client-sdk-python/commit/06869b2316406b875bcdf2535b13c373ae8cecfe))
