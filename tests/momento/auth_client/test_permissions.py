from momento.auth.access_control.disposable_token_scope import (
    CacheItemKey,
    CacheItemKeyPrefix,
    CacheItemSelector,
    DisposableTokenCachePermission,
    DisposableTokenCachePermissions,
    DisposableTokenScope,
)
from momento.auth.access_control.permission_scope import (
    ALL_DATA_READ_WRITE,
    AllCaches,
    AllTopics,
    CachePermission,
    CacheRole,
    CacheSelector,
    Permissions,
    PermissionScope,
    TopicPermission,
    TopicRole,
    TopicSelector,
)
from momento.internal._utilities._permissions import (
    SuperuserPermissions,
    permissions_from_disposable_token_scope,
    permissions_from_permission_scope,
)
from momento_wire_types import permissionmessages_pb2 as permissions_pb

from tests.utils import str_to_bytes


def test_creates_expected_grpc_permissions_for_internal_superuser_permissions() -> None:
    expected_permission = permissions_pb.Permissions(super_user=permissions_pb.SuperUserPermissions.SuperUser)
    constructed_permission = permissions_from_permission_scope(PermissionScope(SuperuserPermissions()))
    assert expected_permission == constructed_permission


def test_creates_expected_grpc_permissions_for_all_data_read_write() -> None:
    expected_topic_permission = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicReadWrite,
        all_topics=permissions_pb.PermissionsType.All(),
        all_caches=permissions_pb.PermissionsType.All(),
    )
    expected_cache_permission = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheReadWrite,
        all_caches=permissions_pb.PermissionsType.All(),
    )
    expected_permission = permissions_pb.Permissions(
        explicit=permissions_pb.ExplicitPermissions(permissions=
            [
                permissions_pb.PermissionsType(cache_permissions=expected_cache_permission),
                permissions_pb.PermissionsType(topic_permissions=expected_topic_permission),
            ]
        )
    )
    constructed_permission = permissions_from_permission_scope(PermissionScope(ALL_DATA_READ_WRITE))
    assert expected_permission == constructed_permission


def test_creates_expected_grpc_permissions_for_cache_and_topic_specific_permissions() -> None:
    grpc_read_any_cache = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheReadOnly,
        all_caches=permissions_pb.PermissionsType.All(),
    )
    grpc_write_cache_foo = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheReadWrite,
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="foo"),
    )
    grpc_read_any_topic = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicReadOnly,
        all_topics=permissions_pb.PermissionsType.All(),
        all_caches=permissions_pb.PermissionsType.All(),
    )
    grpc_read_write_any_topic_in_cache_foo = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicReadWrite,
        all_topics=permissions_pb.PermissionsType.All(),
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="foo"),
    )
    grpc_read_write_topic_bar_in_any_cache = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicReadWrite,
        topic_selector=permissions_pb.PermissionsType.TopicSelector(topic_name="bar"),
        all_caches=permissions_pb.PermissionsType.All(),
    )
    grpc_read_write_topic_cat_in_cache_dog = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicReadWrite,
        topic_selector=permissions_pb.PermissionsType.TopicSelector(topic_name="cat"),
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="dog"),
    )
    expected_permissions = permissions_pb.Permissions(
        explicit=permissions_pb.ExplicitPermissions(permissions=
            [
                permissions_pb.PermissionsType(cache_permissions=grpc_read_any_cache),
                permissions_pb.PermissionsType(cache_permissions=grpc_write_cache_foo),
                permissions_pb.PermissionsType(topic_permissions=grpc_read_any_topic),
                permissions_pb.PermissionsType(topic_permissions=grpc_read_write_any_topic_in_cache_foo),
                permissions_pb.PermissionsType(topic_permissions=grpc_read_write_topic_bar_in_any_cache),
                permissions_pb.PermissionsType(topic_permissions=grpc_read_write_topic_cat_in_cache_dog),
            ]
        )
    )

    constructed_permissions = permissions_from_permission_scope(
        PermissionScope(
            Permissions(
                [
                    CachePermission(role=CacheRole.READ_ONLY, cache_selector=CacheSelector(AllCaches())),
                    CachePermission(role=CacheRole.READ_WRITE, cache_selector=CacheSelector("foo")),
                    TopicPermission(
                        role=TopicRole.SUBSCRIBE_ONLY,
                        cache_selector=CacheSelector(AllCaches()),
                        topic_selector=TopicSelector(AllTopics()),
                    ),
                    TopicPermission(
                        role=TopicRole.PUBLISH_SUBSCRIBE,
                        cache_selector=CacheSelector("foo"),
                        topic_selector=TopicSelector(AllTopics()),
                    ),
                    TopicPermission(
                        role=TopicRole.PUBLISH_SUBSCRIBE,
                        cache_selector=CacheSelector(AllCaches()),
                        topic_selector=TopicSelector("bar"),
                    ),
                    TopicPermission(
                        role=TopicRole.PUBLISH_SUBSCRIBE,
                        cache_selector=CacheSelector("dog"),
                        topic_selector=TopicSelector("cat"),
                    ),
                ]
            )
        )
    )

    assert expected_permissions == constructed_permissions


def test_creates_expected_grpc_permissions_for_write_only_cache_and_topic_permissions() -> None:
    grpc_write_only_all_caches = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheWriteOnly,
        all_caches=permissions_pb.PermissionsType.All(),
    )
    grpc_write_only_cache_foo = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheWriteOnly,
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="foo"),
    )
    grpc_publish_only_all_topics_cache_foo = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicWriteOnly,
        all_topics=permissions_pb.PermissionsType.All(),
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="foo"),
    )
    grpc_publish_only_topic_bar_all_caches = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicWriteOnly,
        topic_selector=permissions_pb.PermissionsType.TopicSelector(topic_name="bar"),
        all_caches=permissions_pb.PermissionsType.All(),
    )
    grpc_publish_only_cache_dog_topic_cat = permissions_pb.PermissionsType.TopicPermissions(
        role=permissions_pb.TopicWriteOnly,
        topic_selector=permissions_pb.PermissionsType.TopicSelector(topic_name="cat"),
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="dog"),
    )
    expected_permissions = permissions_pb.Permissions(
        explicit=permissions_pb.ExplicitPermissions(permissions=
            [
                permissions_pb.PermissionsType(cache_permissions=grpc_write_only_all_caches),
                permissions_pb.PermissionsType(cache_permissions=grpc_write_only_cache_foo),
                permissions_pb.PermissionsType(topic_permissions=grpc_publish_only_all_topics_cache_foo),
                permissions_pb.PermissionsType(topic_permissions=grpc_publish_only_topic_bar_all_caches),
                permissions_pb.PermissionsType(topic_permissions=grpc_publish_only_cache_dog_topic_cat),
            ]
        )
    )

    constructed_permissions = permissions_from_permission_scope(
        PermissionScope(
            Permissions(
                [
                    CachePermission(role=CacheRole.WRITE_ONLY, cache_selector=CacheSelector(AllCaches())),
                    CachePermission(role=CacheRole.WRITE_ONLY, cache_selector=CacheSelector("foo")),
                    TopicPermission(
                        role=TopicRole.PUBLISH_ONLY,
                        cache_selector=CacheSelector("foo"),
                        topic_selector=TopicSelector(AllTopics()),
                    ),
                    TopicPermission(
                        role=TopicRole.PUBLISH_ONLY,
                        cache_selector=CacheSelector(AllCaches()),
                        topic_selector=TopicSelector("bar"),
                    ),
                    TopicPermission(
                        role=TopicRole.PUBLISH_ONLY,
                        cache_selector=CacheSelector("dog"),
                        topic_selector=TopicSelector("cat"),
                    ),
                ]
            )
        )
    )

    assert expected_permissions == constructed_permissions


def test_creates_expected_grpc_permissions_for_key_specific_write_only_cache_and_topic_specific_permissions() -> None:
    grpc_write_only_all_caches_specific_key = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheWriteOnly,
        all_caches=permissions_pb.PermissionsType.All(),
        item_selector=permissions_pb.PermissionsType.CacheItemSelector(key=str_to_bytes("specific_key")),
    )
    grpc_write_only_cache_foo_specific_key_prefix = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheWriteOnly,
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="foo"),
        item_selector=permissions_pb.PermissionsType.CacheItemSelector(key_prefix=str_to_bytes("specific_key_prefix")),
    )
    expected_permissions = permissions_pb.Permissions(
        explicit=permissions_pb.ExplicitPermissions(permissions=
            [
                permissions_pb.PermissionsType(cache_permissions=grpc_write_only_all_caches_specific_key),
                permissions_pb.PermissionsType(cache_permissions=grpc_write_only_cache_foo_specific_key_prefix),
            ]
        )
    )

    constructed_permissions = permissions_from_disposable_token_scope(
        DisposableTokenScope(
            DisposableTokenCachePermissions(
                [
                    DisposableTokenCachePermission(
                        CacheSelector(AllCaches()),
                        CacheRole.WRITE_ONLY,
                        CacheItemSelector(CacheItemKey("specific_key")),
                    ),
                    DisposableTokenCachePermission(
                        CacheSelector("foo"),
                        CacheRole.WRITE_ONLY,
                        CacheItemSelector(CacheItemKeyPrefix("specific_key_prefix")),
                    ),
                ]
            )
        )
    )

    assert expected_permissions == constructed_permissions


def test_creates_expected_grpc_permissions_for_key_specific_read_only_cache_and_topic_specific_permissions() -> None:
    grpc_read_only_all_caches_specific_key = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheReadOnly,
        all_caches=permissions_pb.PermissionsType.All(),
        item_selector=permissions_pb.PermissionsType.CacheItemSelector(key=str_to_bytes("specific_key")),
    )
    grpc_read_only_cache_foo_specific_key_prefix = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheReadOnly,
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="foo"),
        item_selector=permissions_pb.PermissionsType.CacheItemSelector(key_prefix=str_to_bytes("specific_key_prefix")),
    )
    expected_permissions = permissions_pb.Permissions(
        explicit=permissions_pb.ExplicitPermissions(permissions=
            [
                permissions_pb.PermissionsType(cache_permissions=grpc_read_only_all_caches_specific_key),
                permissions_pb.PermissionsType(cache_permissions=grpc_read_only_cache_foo_specific_key_prefix),
            ]
        )
    )

    constructed_permissions = permissions_from_disposable_token_scope(
        DisposableTokenScope(
            DisposableTokenCachePermissions(
                [
                    DisposableTokenCachePermission(
                        CacheSelector(AllCaches()),
                        CacheRole.READ_ONLY,
                        CacheItemSelector(CacheItemKey("specific_key")),
                    ),
                    DisposableTokenCachePermission(
                        CacheSelector("foo"),
                        CacheRole.READ_ONLY,
                        CacheItemSelector(CacheItemKeyPrefix("specific_key_prefix")),
                    ),
                ]
            )
        )
    )

    assert expected_permissions == constructed_permissions


def test_creates_expected_grpc_permissions_for_key_specific_read_write_cache_and_topic_specific_permissions() -> None:
    grpc_read_write_all_caches_specific_key = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheReadWrite,
        all_caches=permissions_pb.PermissionsType.All(),
        item_selector=permissions_pb.PermissionsType.CacheItemSelector(key=str_to_bytes("specific_key")),
    )
    grpc_read_write_cache_foo_specific_key_prefix = permissions_pb.PermissionsType.CachePermissions(
        role=permissions_pb.CacheReadWrite,
        cache_selector=permissions_pb.PermissionsType.CacheSelector(cache_name="foo"),
        item_selector=permissions_pb.PermissionsType.CacheItemSelector(key_prefix=str_to_bytes("specific_key_prefix")),
    )
    expected_permissions = permissions_pb.Permissions(
        explicit=permissions_pb.ExplicitPermissions(permissions=
            [
                permissions_pb.PermissionsType(cache_permissions=grpc_read_write_all_caches_specific_key),
                permissions_pb.PermissionsType(cache_permissions=grpc_read_write_cache_foo_specific_key_prefix),
            ]
        )
    )

    constructed_permissions = permissions_from_disposable_token_scope(
        DisposableTokenScope(
            DisposableTokenCachePermissions(
                [
                    DisposableTokenCachePermission(
                        CacheSelector(AllCaches()),
                        CacheRole.READ_WRITE,
                        CacheItemSelector(CacheItemKey("specific_key")),
                    ),
                    DisposableTokenCachePermission(
                        CacheSelector("foo"),
                        CacheRole.READ_WRITE,
                        CacheItemSelector(CacheItemKeyPrefix("specific_key_prefix")),
                    ),
                ]
            )
        )
    )

    assert expected_permissions == constructed_permissions
