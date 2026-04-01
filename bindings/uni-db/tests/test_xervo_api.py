"""Tests for xervo API design: type names, builder chaining, dict messages, generate_text."""

import pytest

import uni_db
from uni_db import AsyncUniBuilder, Message, UniBuilder


class TestXervoTypeNames:
    def test_message_class_name(self):
        assert Message.__name__ == "Message"

    def test_token_usage_class_name(self):
        assert uni_db.TokenUsage.__name__ == "TokenUsage"

    def test_generation_result_class_name(self):
        assert uni_db.GenerationResult.__name__ == "GenerationResult"

    def test_generation_options_removed(self):
        assert not hasattr(uni_db, "GenerationOptions")
        assert not hasattr(uni_db, "PyGenerationOptions")


class TestMessageApi:
    def test_message_constructor(self):
        msg = Message("user", "hello")
        assert msg.role == "user"
        assert msg.content == "hello"

    def test_message_user_factory(self):
        msg = Message.user("hi")
        assert msg.role == "user"
        assert msg.content == "hi"

    def test_message_assistant_factory(self):
        msg = Message.assistant("hi")
        assert msg.role == "assistant"

    def test_message_system_factory(self):
        msg = Message.system("hi")
        assert msg.role == "system"

    def test_message_repr(self):
        msg = Message("user", "hello")
        assert repr(msg).startswith("Message(role=")


class TestBuilderChaining:
    def test_config_returns_self(self):
        builder = UniBuilder.temporary()
        result = builder.config({"query_timeout": 5.0})
        assert result is builder
        db = result.build()
        assert db is not None

    def test_cloud_config_returns_self(self):
        builder = UniBuilder.temporary()
        result = builder.cloud_config({"provider": "s3", "bucket": "test"})
        assert result is builder

    def test_config_chain_with_other_methods(self):
        db = (
            UniBuilder.temporary()
            .config({"parallelism": 2})
            .cache_size(1024 * 1024)
            .build()
        )
        assert db is not None


class TestAsyncBuilderChaining:
    async def test_config_returns_self(self):
        builder = AsyncUniBuilder.temporary()
        result = builder.config({"query_timeout": 5.0})
        assert result is builder
        db = await result.build()
        assert db is not None

    async def test_cloud_config_returns_self(self):
        builder = AsyncUniBuilder.temporary()
        result = builder.cloud_config({"provider": "s3", "bucket": "test"})
        assert result is builder

    async def test_config_chain_with_other_methods(self):
        db = await (
            AsyncUniBuilder.temporary()
            .config({"parallelism": 2})
            .cache_size(1024 * 1024)
            .build()
        )
        assert db is not None


class TestGenerateDictMessages:
    def test_generate_accepts_message_objects(self, empty_db):
        with pytest.raises(uni_db.UniInternalError):
            empty_db.xervo().generate("m", [Message.user("hi")])

    def test_generate_accepts_dicts(self, empty_db):
        with pytest.raises(uni_db.UniInternalError):
            empty_db.xervo().generate("m", [{"role": "user", "content": "hi"}])

    def test_generate_accepts_mixed(self, empty_db):
        with pytest.raises(uni_db.UniInternalError):
            empty_db.xervo().generate(
                "m", [Message.user("hi"), {"role": "assistant", "content": "hello"}]
            )

    def test_generate_rejects_bad_type(self, empty_db):
        with pytest.raises(TypeError, match=r"messages\[0\]"):
            empty_db.xervo().generate("m", [42])

    def test_generate_rejects_dict_missing_role(self, empty_db):
        with pytest.raises(TypeError, match=r"'role'"):
            empty_db.xervo().generate("m", [{"content": "hi"}])

    def test_generate_rejects_dict_missing_content(self, empty_db):
        with pytest.raises(TypeError, match=r"'content'"):
            empty_db.xervo().generate("m", [{"role": "user"}])


class TestGenerateText:
    def test_generate_text_exists(self, empty_db):
        with pytest.raises(uni_db.UniInternalError):
            empty_db.xervo().generate_text("m", "hello")

    def test_generate_text_with_options(self, empty_db):
        with pytest.raises(uni_db.UniInternalError):
            empty_db.xervo().generate_text(
                "m", "hello", max_tokens=100, temperature=0.7
            )


class TestAsyncGenerateDictMessages:
    async def test_generate_accepts_message_objects(self, async_empty_db):
        with pytest.raises(uni_db.UniInternalError):
            await async_empty_db.xervo().generate("m", [Message.user("hi")])

    async def test_generate_accepts_dicts(self, async_empty_db):
        with pytest.raises(uni_db.UniInternalError):
            await async_empty_db.xervo().generate(
                "m", [{"role": "user", "content": "hi"}]
            )

    async def test_generate_accepts_mixed(self, async_empty_db):
        with pytest.raises(uni_db.UniInternalError):
            await async_empty_db.xervo().generate(
                "m", [Message.user("hi"), {"role": "assistant", "content": "hello"}]
            )

    async def test_generate_rejects_bad_type(self, async_empty_db):
        with pytest.raises(TypeError, match=r"messages\[0\]"):
            async_empty_db.xervo().generate("m", [42])

    async def test_generate_rejects_dict_missing_role(self, async_empty_db):
        with pytest.raises(TypeError, match=r"'role'"):
            async_empty_db.xervo().generate("m", [{"content": "hi"}])

    async def test_generate_rejects_dict_missing_content(self, async_empty_db):
        with pytest.raises(TypeError, match=r"'content'"):
            async_empty_db.xervo().generate("m", [{"role": "user"}])


class TestAsyncGenerateText:
    async def test_generate_text_exists(self, async_empty_db):
        with pytest.raises(uni_db.UniInternalError):
            await async_empty_db.xervo().generate_text("m", "hello")

    async def test_generate_text_with_options(self, async_empty_db):
        with pytest.raises(uni_db.UniInternalError):
            await async_empty_db.xervo().generate_text(
                "m", "hello", max_tokens=100, temperature=0.7
            )
