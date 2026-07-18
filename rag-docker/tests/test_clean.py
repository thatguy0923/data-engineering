"""测试 clean_validate.py 的纯函数——不需要 Spark，只测逻辑。"""
import pytest
from pipeline.clean_validate import to_halfwidth, detect_lang


class TestToHalfwidth:
    """NFKC 规范化：全角 → 半角"""

    def test_halfwidth_numbers(self):
        """全角数字 → 半角数字"""
        assert to_halfwidth("１２３") == "123"

    def test_halfwidth_letters(self):
        """全角英文字母 → 半角"""
        assert to_halfwidth("ＡＢＣ") == "ABC"

    def test_japanese_text_unchanged(self):
        """日文假名不变（本来就是半角）"""
        assert to_halfwidth("ありがとう") == "ありがとう"

    def test_none_returns_none(self):
        """None 安全处理"""
        assert to_halfwidth(None) is None


class TestDetectLang:
    """Unicode 范围语言检测"""

    def test_hiragana_is_jpn(self):
        """平假名 → jpn"""
        assert detect_lang("ありがとう") == "jpn"

    def test_katakana_is_jpn(self):
        """片假名 → jpn"""
        assert detect_lang("アルバイト") == "jpn"

    def test_hangul_is_kor(self):
        """谚文 → kor"""
        assert detect_lang("감사합니다") == "kor"

    def test_english_is_other(self):
        """英文 → other"""
        assert detect_lang("hello world") == "other"

    def test_none_is_other(self):
        """None → other"""
        assert detect_lang(None) == "other"
