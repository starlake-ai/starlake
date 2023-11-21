// istanbul ignore file
import React from "react";
import clsx from "clsx";
import styles from "./LoadingRing.module.css";
export default function LoadingRing({ className, }) {
    return (<div className={clsx(styles.loadingRing, className)}>
      <div></div>
      <div></div>
      <div></div>
      <div></div>
    </div>);
}
