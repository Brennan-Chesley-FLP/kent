"""Interstitial page handlers for the Playwright driver.

Interstitial handlers run on the live Playwright page after navigation
but before the DOM snapshot is taken.  The driver races each handler's
waitlist against the scraper step's own await_list; if a handler's
conditions match first, it gets to interact with the page (e.g. solve a
captcha) before the scraper ever sees the HTML.
"""

from __future__ import annotations

import abc
import asyncio
import logging
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import httpx

from kent.data_types import (
    DriverRequirement,
    WaitForLoadState,
    WaitForSelector,
    WaitForTimeout,
    WaitForURL,
)

if TYPE_CHECKING:
    from playwright.async_api import FrameLocator, Page, Response

logger = logging.getLogger(__name__)

WaitCondition = (
    WaitForSelector | WaitForLoadState | WaitForURL | WaitForTimeout
)


@runtime_checkable
class AudioTranscriber(Protocol):
    """Protocol for audio-to-text transcription services.

    Implementations receive raw audio bytes and return the transcribed
    text.  Used by :class:`ReCaptchaHandler` to solve audio challenges.
    """

    async def transcribe(self, audio_data: bytes) -> str: ...


class LocalStenoTranscriber:
    """AudioTranscriber backed by a local steno transcription server.

    Posts audio bytes to the steno server's ``/transcribe`` endpoint
    and returns the plain-text transcription.

    Args:
        server_url: Base URL of the steno server.
            Defaults to ``http://127.0.0.1:8000``.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        server_url: str = "http://127.0.0.1:8000",
        timeout: float = 30.0,
    ) -> None:
        self._server_url = server_url.rstrip("/")
        self._timeout = timeout

    async def transcribe(self, audio_data: bytes) -> str:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._server_url}/transcribe",
                params={"format": "text"},
                files={
                    "file": ("audio.mp3", audio_data, "audio/mpeg"),
                },
            )
            response.raise_for_status()
            return response.text.strip()


class InterstitialHandler(abc.ABC):
    """Handles interstitial pages (captchas, disclaimers, etc.) on the live
    Playwright page, after navigation but before DOM snapshot."""

    @abc.abstractmethod
    def waitlist(self) -> list[WaitCondition]:
        """Conditions that indicate this interstitial is present.

        All conditions must match (conjunction) for the handler to fire.
        """

    @abc.abstractmethod
    async def navigate_through(self, page: Page) -> None:
        """Interact with the live page to get past the interstitial.

        When this returns, the page should be showing the real content
        (or another interstitial that a subsequent handler can deal with).
        """


class HCaptchaHandler(InterstitialHandler):
    """Handles hCaptcha interstitial pages.

    Clicks the ``div.h-captcha`` element, which triggers the hCaptcha
    widget.  In headless Firefox with ``navigator.webdriver`` overridden,
    this auto-solves; the JS callback then submits the form, navigating
    to the real content page.
    """

    def waitlist(self) -> list[WaitCondition]:
        return [WaitForSelector("div.h-captcha")]

    async def navigate_through(self, page: Page) -> None:
        logger.info("hCaptcha interstitial detected — clicking to solve")
        captcha = page.locator("div.h-captcha")
        await captcha.click()
        await page.wait_for_load_state("networkidle")


class ReCaptchaHandler(InterstitialHandler):
    """Handles reCAPTCHA v2 interstitials via the audio challenge.

    Detects a ``div.g-recaptcha`` widget (even when hidden behind
    invisible parents), switches to the audio challenge, downloads
    the audio clip, transcribes it via the provided
    :class:`AudioTranscriber`, and submits the answer.

    Args:
        transcriber: An :class:`AudioTranscriber` implementation for
            converting audio challenge clips to text.
    """

    def __init__(self, transcriber: AudioTranscriber) -> None:
        self._transcriber = transcriber

    def waitlist(self) -> list[WaitCondition]:
        return [WaitForSelector("div.g-recaptcha", state="attached")]

    async def navigate_through(self, page: Page) -> None:
        logger.info("reCAPTCHA interstitial detected — solving via audio")

        # 1. Reveal hidden parent elements of .g-recaptcha
        await page.evaluate("""() => {
            const el = document.querySelector('.g-recaptcha');
            if (!el) return;
            let node = el.parentElement;
            while (node && node !== document.body) {
                node.style.display = '';
                node.style.visibility = '';
                const cs = window.getComputedStyle(node);
                if (cs.display === 'none') node.style.display = 'block';
                if (cs.visibility === 'hidden')
                    node.style.visibility = 'visible';
                node = node.parentElement;
            }
        }""")

        # 2. Click the reCAPTCHA checkbox inside the anchor iframe
        anchor = page.frame_locator(
            "iframe[src*='google.com/recaptcha'][src*='anchor']"
        )
        await anchor.locator("#recaptcha-anchor").click(timeout=10_000)

        # 3. Race: auto-solve (checkmark appears) vs challenge (bframe)
        async def _wait_for_checkmark() -> str:
            await anchor.locator(".recaptcha-checkbox-checked").wait_for(
                state="attached", timeout=10_000
            )
            return "solved"

        async def _wait_for_bframe() -> str:
            await page.locator(
                "iframe[src*='google.com/recaptcha'][src*='bframe']"
            ).wait_for(state="attached", timeout=10_000)
            return "challenge"

        tasks = {
            asyncio.create_task(_wait_for_checkmark()),
            asyncio.create_task(_wait_for_bframe()),
        }
        try:
            done, _ = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        outcome = next(iter(done)).result()
        if outcome == "solved":
            logger.info("reCAPTCHA auto-solved (no challenge)")
            return

        # 4. Click the audio challenge button inside the bframe
        bframe = page.frame_locator(
            "iframe[src*='google.com/recaptcha'][src*='bframe']"
        )
        await bframe.locator("#recaptcha-audio-button").click(timeout=10_000)

        # 5. Click PLAY and intercept the audio response
        await bframe.locator(".rc-audiochallenge-tdownload-link").wait_for(
            state="attached", timeout=15_000
        )

        audio_response = await self._intercept_audio(page, bframe)
        audio_data = await audio_response.body()

        # 6. Transcribe the audio
        logger.debug(
            "Transcribing reCAPTCHA audio (%d bytes)",
            len(audio_data),
        )
        transcription = await self._transcriber.transcribe(audio_data)
        logger.info("reCAPTCHA audio transcription: %r", transcription)

        # 7. Fill the response field and submit
        await bframe.locator("#audio-response").fill(transcription)
        await bframe.locator("#recaptcha-verify-button").click(timeout=10_000)

        # 8. Verify success — checkmark visible in the anchor iframe
        await anchor.locator(".recaptcha-checkbox-checkmark").wait_for(
            state="visible", timeout=15_000
        )
        logger.info("reCAPTCHA solved successfully")

    @staticmethod
    async def _intercept_audio(
        page: Page,
        bframe: FrameLocator,
    ) -> Response:
        """Click PLAY and intercept the audio payload response.

        Uses ``page.expect_response`` to capture the audio file as
        the browser fetches it, avoiding a separate HTTP request.
        """
        async with page.expect_response(
            lambda r: "recaptcha" in r.url and "payload" in r.url,
            timeout=15_000,
        ) as response_info:
            await bframe.locator(".rc-audiochallenge-play-button").click(
                timeout=10_000
            )
        return await response_info.value


INTERSTITIAL_HANDLERS: dict[DriverRequirement, InterstitialHandler] = {
    DriverRequirement.HCAP_HANDLER: HCaptchaHandler(),
    DriverRequirement.RCAP_HANDLER: ReCaptchaHandler(LocalStenoTranscriber()),
}
