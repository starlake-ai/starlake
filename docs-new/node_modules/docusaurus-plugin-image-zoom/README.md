# docusaurus-plugin-image-zoom

> Docusaurus plugin to utilize [`medium-zoom`](https://www.npmjs.com/package/medium-zoom) in your projects easily!

## Install

```shell
npm install docusaurus-plugin-image-zoom
```

## Usage

Add as a plugin to `docusaurus.config.js`:

```js
  plugins: [
    require.resolve('docusaurus-plugin-image-zoom')
  ],
```

## Configuration

This plugin allows you to configure the following options:

| Option        | Description                                                                                      | Default Value                                                                                         |
|---------------|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `selector`    | (optional) The selector to use for the images to zoom.                                          | `.markdown img`                                                                                      |
| `background`  | (optional) The background color to use for the zoomed image.                                     | `{ light: 'rgb(255, 255, 255)', dark: 'rgb(50, 50, 50)' }`                                           |
| `config`      | (optional) The configuration object to pass to `medium-zoom`.                                   | `{}`                                                                                                 |


Example usage in `docusaurus.config.js`:

```js
  themeConfig: {
    zoom: {
      selector: '.markdown :not(em) > img',
      background: {
        light: 'rgb(255, 255, 255)',
        dark: 'rgb(50, 50, 50)'
      },
      config: {
        // options you can specify via https://github.com/francoischalifour/medium-zoom#usage
      }
    }
  },
```
