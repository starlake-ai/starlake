import siteConfig from '@generated/docusaurus.config';
import mediumZoom, { Zoom, ZoomOptions, ZoomSelector } from 'medium-zoom';

const { themeConfig } = siteConfig;

type ZoomConfig = {
  selector: ZoomSelector;
  background: {
    light: string;
    dark: string;
  };
  config: ZoomOptions;
};

function getBackgroundColor(zoom: ZoomConfig) {
  const isDarkMode = document.querySelector('html[data-theme="dark"]');

  return isDarkMode
    ? zoom.background?.dark || 'rgb(50, 50, 50)'
    : zoom.background?.light || 'rgb(255, 255, 255)';
}

export default (function () {
  if (typeof window === 'undefined') {
    return null;
  }

  let zoomObject: Zoom;

  const { zoom }: { zoom?: ZoomConfig } = themeConfig;
  const { selector = '.markdown img', config = {} } = zoom || ({} as ZoomConfig);

  if (!zoom) {
    return null;
  }

  config.background = getBackgroundColor(zoom);

  var observer = new MutationObserver(function () {
    if (!zoomObject) {
      return;
    }

    zoomObject.update({ background: getBackgroundColor(zoom) });
  });

  const htmlNode = document.querySelector('html');

  observer.observe(htmlNode!, {
    attributes: true,
    attributeFilter: ['data-theme'],
  });

  setTimeout(() => {
    if (zoomObject) {
      zoomObject.detach();
    }

    zoomObject = mediumZoom(selector, config);
  }, 1000);

  return {
    onRouteUpdate() {
      setTimeout(() => {
        if (zoomObject) {
          zoomObject.detach();
        }

        zoomObject = mediumZoom(selector, config);
      }, 1000);
    },
  };
})();
