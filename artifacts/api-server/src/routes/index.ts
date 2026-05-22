import { Router, type IRouter } from "express";
import healthRouter from "./health.js";
import candlesRouter from "./candles.js";
import instrumentsRouter from "./instruments.js";
import latestRouter from "./latest.js";
import apiIndexRouter from "./api-index.js";

const router: IRouter = Router();

router.use(apiIndexRouter);
router.use(healthRouter);
router.use(candlesRouter);
router.use(instrumentsRouter);
router.use(latestRouter);

export default router;
